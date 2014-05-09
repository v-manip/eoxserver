

import os 
import os.path
from uuid import uuid4
from datetime import datetime
import struct
import csv
from StringIO import StringIO

import numpy as np 

from django.contrib.gis.geos import Point, MultiPoint, Polygon
from django.contrib.gis.gdal import SpatialReference
from django.db.models import Q

from eoxserver.core import Component, ExtensionPoint, implements
from eoxserver.core.util.timetools import isoformat
from eoxserver.core.util.rect import Rect
from eoxserver.contrib import gdal
from eoxserver.backends.access import connect
from eoxserver.services.ows.wps.interfaces import ProcessInterface
from eoxserver.services.ows.wps.parameters import LiteralData, ComplexData
from eoxserver.resources.coverages import models
from eoxserver.services.subset import Subsets, Trim

import logging

logger = logging.getLogger(__name__)


class GetPixelValues(Component):
    implements(ProcessInterface)

    identifier = "getdata"
    title = "Pixel Value extractor"
    description = "Creates a string output in csv style with the defined number of random generated values."
    metadata = ["a", "b"]
    profiles = ["p", "q"]

    inputs = {
        "collection": str,
        "begin_time": datetime,
        "end_time": datetime,
        "coord_list": str,
        "srid": int
    }

    outputs = {
        "processed": str
    }

    def execute(self, collection, begin_time, end_time, coord_list, srid):
        """ The main execution function for the process.
        """
        col_name = collection
        collection = models.Collection.objects.get(identifier=collection)

        eo_objects = collection.eo_objects.filter(
            begin_time__lte=end_time, end_time__gte=begin_time
        )

        coordinates = coord_list.split(';')

        points = []
        for coordinate in coordinates:
            x,y = coordinate.split(',')
            # parameter parsing
            point = Point(float(x), float(y))
            point.srid = srid
            points.append(point)

        points = MultiPoint(points)
        points.srid = srid


        eo_objects = eo_objects.filter(
            footprint__intersects=points
        )

        output = StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_ALL)
        #header = ["id", "begin", "end"] + ["point%d" % i for i in range(len(points))]
        header = ["id", "Green", "Red", "NIR", "MIR" ]
        writer.writerow(header)

        for eo_object in eo_objects:
            coverage = eo_object.cast()

            #values = [coverage.identifier, isoformat(coverage.begin_time), isoformat(coverage.end_time)] + [None] * len(points)
            values = [collection] + [None] * 4

            data_item = coverage.data_items.get(semantic__startswith="bands")
            filename = connect(data_item)
            ds = gdal.Open(filename)
            sr = SpatialReference(ds.GetProjection())
            #points_t = points.transform(sr, clone=True)

            for index, point in enumerate(points, start=1):

                if not coverage.footprint.contains(point):
                    continue

                gt = ds.GetGeoTransform()

                point.transform(sr)
         
                # Works only if gt[2] and gt[4] equal zero! 
                px = int((point[0] - gt[0]) / gt[1]) #x pixel
                py = int((point[1] - gt[3]) / gt[5]) #y pixel

                #array = ds.ReadRaster(px, py, 1, 1)
                #structval = ds.ReadRaster(px,py,1,1,buf_type=gdal.GDT_Int16) #TODO: Check Range Type to adapt buf_type!
                pixelVal = ds.ReadAsArray(px,py,1,1)[:,0,0]

                #pixel_value = array[0][0]
                #print structval
                #pixel_value = struct.unpack('IIII' , structval) #use the 'short' format code (2 bytes) not int (4 bytes)
                
                #values[index] = pixel_value[0]
                #writer.writerow([ col_name+"_p"+str(index), pixelVal[0], pixelVal[1], pixelVal[2], pixelVal[3] ])
                writer.writerow([ "P_"+str(index), pixelVal[0], pixelVal[1], pixelVal[2], pixelVal[3] ])

        return {
            "processed": output.getvalue()
        }




class GetTimeDataProcess(Component):
    implements(ProcessInterface)

    identifier = "getTimeData"
    title = "Retrieves time information about a collection"
    description = "Creates csv output of coverage time information of collections."
    metadata = ["a", "b"]
    profiles = ["p", "q"]

    inputs = {
        "collection": str,
        "begin_time": datetime,
        "end_time": datetime
    }

    outputs = {
        "times": str
    }

    def execute(self, collection, begin_time, end_time):
        """ The main execution function for the process.
        """

        eo_ids = [collection]

        
        containment = "overlaps"

        subsets = Subsets((Trim("t", begin_time, end_time),))


        if len(eo_ids) == 0:
            raise

        # fetch a list of all requested EOObjects
        available_ids = models.EOObject.objects.filter(
            identifier__in=eo_ids
        ).values_list("identifier", flat=True)

        # match the requested EOIDs against the available ones. If any are
        # requested, that are not available, raise and exit.
        failed = [ eo_id for eo_id in eo_ids if eo_id not in available_ids ]
        if failed:
            raise NoSuchDatasetSeriesOrCoverageException(failed)

        collections_qs = subsets.filter(models.Collection.objects.filter(
            identifier__in=eo_ids
        ), containment="overlaps")

        # create a set of all indirectly referenced containers by iterating
        # recursively. The containment is set to "overlaps", to also include 
        # collections that might have been excluded with "contains" but would 
        # have matching coverages inserted.

        def recursive_lookup(super_collection, collection_set):
            sub_collections = models.Collection.objects.filter(
                collections__in=[super_collection.pk]
            ).exclude(
                pk__in=map(lambda c: c.pk, collection_set)
            )
            sub_collections = subsets.filter(sub_collections, "overlaps")

            # Add all to the set
            collection_set |= set(sub_collections)

            for sub_collection in sub_collections:
                recursive_lookup(sub_collection, collection_set)

        collection_set = set(collections_qs)
        for collection in set(collection_set):
            recursive_lookup(collection, collection_set)

        collection_pks = map(lambda c: c.pk, collection_set)

        # Get all either directly referenced coverages or coverages that are
        # within referenced containers. Full subsetting is applied here.

        coverages_qs = subsets.filter(models.Coverage.objects.filter(
            Q(identifier__in=eo_ids) | Q(collections__in=collection_pks)
        ), containment=containment)

       

        output = StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_ALL)
        header = ["starttime", "endtime", "bbox", "identifier" ]
        writer.writerow(header)

        for coverage in coverages_qs:
            starttime = coverage.begin_time
            endtime = coverage.end_time
            identifier = coverage.identifier
            bbox = coverage.extent_wgs84
            writer.writerow([isoformat(starttime), isoformat(endtime), bbox, identifier])


        return output.getvalue()
        


class GetCoverageDifference(Component):
    implements(ProcessInterface)

    identifier = "getCoverageDifference"
    title = "Difference image computation"
    description = "Creates difference of two coverages"
    metadata = ["a", "b"]
    profiles = ["p", "q"]

    inputs = {
        "master_id": str,
        "slave_id": str,
        "bbox": str,
        "crs": int
    }

    outputs = {
        "processed": str
    }

    def execute(self, master_id, slave_id, bbox, crs):
        """ The main execution function for the process.
        """
    
        master = models.RectifiedDataset.objects.get(identifier=master_id)
        slave = models.RectifiedDataset.objects.get(identifier=slave_id)

        filename_master = connect(master.data_items.get(semantic__startswith="bands"))
        filename_slave = connect(slave.data_items.get(semantic__startswith="bands"))

        ds_master = gdal.Open(filename_master)
        ds_slave = gdal.Open(filename_slave)


        # bbox in pixels 
        
        rm = Rect(0,0,ds_master.RasterXSize,ds_master.RasterYSize)
        rs = Rect(0,0,ds_slave.RasterXSize,ds_slave.RasterYSize)

        ext = rm & rs 

        pix_master = np.array(ds_master.ReadAsArray(ext.offset_x,ext.offset_y,ext.size_x,ext.size_y),'float32')
        pix_slave  = np.array(ds_slave.ReadAsArray(ext.offset_x,ext.offset_y,ext.size_x,ext.size_y),'float32')

        #pix_res    = pix_master - pix_slave
    
        def _diff(a,b):
            c = np.zeros((a.shape[1],a.shape[2]))
            for i in xrange(a.shape[0]):
                c[:,:] += ( a[i,:,:] - b[i,:,:] )**2
            return np.sqrt(c)

        pix_res = _diff(pix_master, pix_slave)

        max_pix = np.max(pix_res)
        scale = 254.0/max_pix if max_pix > 0 else 1.0

        pix_res = np.array(pix_res*scale+1,'uint8')

        print "%s: pix_res.shape=%s"%(self.identifier,repr(pix_res.shape))

        # the output image

        driver = gdal.GetDriverByName('GTiff')

        filename = "/tmp/%s_%s.tif" %( self.identifier,uuid4().hex )

        try:
         
            ds_res = driver.Create(filename,ext.size_x,ext.size_y,1,gdal.GDT_Byte)

            ds_res.GetRasterBand(1).WriteArray(pix_res,0,0)

            with open(filename) as f:
                output = f.read()

        except Exception as e: 

            if os.path.isfile(filename):
                os.remove(filename)

            raise e
           
        else: 
            pass
            #os.remove(filename)
        

        #maxmax = max(max(0,np.max(pix_res),abs(min(0,np.min(pix_res)))))
        #scale = 1.0/maxmax if maxmax > 0 else 1.0 
        #pix_res = np.array(127.0*(pix_res*scale+1)+1,'uint8')


        return {
            "processed": output
        }






        '''

        bbox_p = bbox.split(',')

        polygon = Polygon((bbox_p[0],bbox_p[1]), (bbox_p[0],bbox_p[0]), (bbox_p[0],bbox_p[0]), (bbox_p[0],bbox_p[0]))

        points = []
        for coordinate in coordinates:
            x,y = coordinate.split(',')
            # parameter parsing
            point = Point(float(x), float(y))
            point.srid = srid
            points.append(point)

        points = MultiPoint(points)
        points.srid = srid


        eo_objects = eo_objects.filter(
            footprint__intersects=points
        )

        output = StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_ALL)
        #header = ["id", "begin", "end"] + ["point%d" % i for i in range(len(points))]
        header = ["id", "Green", "Red", "NIR", "MIR" ]
        writer.writerow(header)

        for eo_object in eo_objects:
            coverage = eo_object.cast()

            #values = [coverage.identifier, isoformat(coverage.begin_time), isoformat(coverage.end_time)] + [None] * len(points)
            values = [collection] + [None] * 4

            data_item = coverage.data_items.get(semantic__startswith="bands")
            filename = connect(data_item)
            ds = gdal.Open(filename)
            sr = SpatialReference(ds.GetProjection())
            #points_t = points.transform(sr, clone=True)

            for index, point in enumerate(points, start=1):

                if not coverage.footprint.contains(point):
                    continue

                gt = ds.GetGeoTransform()

                point.transform(sr)
         
                # Works only if gt[2] and gt[4] equal zero! 
                px = int((point[0] - gt[0]) / gt[1]) #x pixel
                py = int((point[1] - gt[3]) / gt[5]) #y pixel

                #array = ds.ReadRaster(px, py, 1, 1)
                #structval = ds.ReadRaster(px,py,1,1,buf_type=gdal.GDT_Int16) #TODO: Check Range Type to adapt buf_type!
                pixelVal = ds.ReadAsArray(px,py,1,1)[:,0,0]

                #pixel_value = array[0][0]
                #print structval
                #pixel_value = struct.unpack('IIII' , structval) #use the 'short' format code (2 bytes) not int (4 bytes)
                
                #values[index] = pixel_value[0]
                #writer.writerow([ col_name+"_p"+str(index), pixelVal[0], pixelVal[1], pixelVal[2], pixelVal[3] ])
                writer.writerow([ "P_"+str(index), pixelVal[0], pixelVal[1], pixelVal[2], pixelVal[3] ])

        return {
            "processed": output.getvalue()
        }
        '''
