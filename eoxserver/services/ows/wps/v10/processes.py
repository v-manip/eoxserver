

import os 
import os.path
import math
import base64
from uuid import uuid4
from datetime import datetime
import struct
import csv
from StringIO import StringIO
from osgeo import gdal, gdalconst, osr

import numpy as np 

from django.contrib.gis.geos import Point, MultiPoint, Polygon
from django.contrib.gis.gdal import SpatialReference
from django.db.models import Q

from eoxserver.core import Component, ExtensionPoint, implements
from eoxserver.core.util.timetools import isoformat
from eoxserver.core.util.rect import Rect
from eoxserver.contrib import gdal
from eoxserver.contrib.vrt import VRTBuilder
from eoxserver.backends.access import connect
from eoxserver.services.ows.wps.interfaces import ProcessInterface
from eoxserver.services.ows.wps.parameters import LiteralData, ComplexData
from eoxserver.services.subset import Subsets, Trim
from eoxserver.resources.coverages import models
from eoxserver.resources.coverages import crss




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
        "collections": str,
        "begin_time": datetime,
        "end_time": datetime,
        "bbox": str,
        "crs": int
    }

    outputs = {
        "processed": str
    }

    def execute(self, collections, begin_time, end_time, bbox, crs):
        """ The main execution function for the process.
        """

        eo_ids = collections.split(',')

        
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


        #for coverage in coverages_qs:
        bbox = map(float, bbox.split(','))
        bbox_poly=Polygon.from_bbox(tuple(bbox))

        coverages_qs =  coverages_qs.filter(footprint__intersects=bbox_poly)

        if len(coverages_qs) < 2:
            raise


        return {
            "processed": diff_process(self, coverages_qs[0].identifier, coverages_qs[1].identifier, bbox, 3, crs)
        }




def diff_process(self, master_id, slave_id, bbox, num_bands, crs):
    """ The main execution function for the process.
    """

    #srid = crss.parseEPSGCode(str(crs), (crss.fromShortCode, crss.fromURN, crss.fromURL))

    master = models.RectifiedDataset.objects.get(identifier=master_id)
    slave = models.RectifiedDataset.objects.get(identifier=slave_id)

    filename_master = connect(master.data_items.get(semantic__startswith="bands"))
    filename_slave = connect(slave.data_items.get(semantic__startswith="bands"))

    ds_master = gdal.Open(filename_master, gdalconst.GA_ReadOnly)
    ds_slave = gdal.Open(filename_slave, gdalconst.GA_ReadOnly)

    master_bbox = master.footprint.extent
    slave_bbox = slave.footprint.extent

    res_x_master = (master_bbox[2] - master_bbox[0]) / ds_master.RasterXSize
    res_y_master = (master_bbox[1] - master_bbox[3]) / ds_master.RasterYSize

    res_x_slave = (slave_bbox[2] - slave_bbox[0]) / ds_slave.RasterXSize
    res_y_slave = (slave_bbox[1] - slave_bbox[3]) / ds_slave.RasterYSize

    size_x = (int((bbox[2]-bbox[0])/res_x_master))
    size_y = (int((bbox[1]-bbox[3])/res_y_master))

    builder = VRTBuilder(size_x, size_y, (num_bands*2))

    dst_rect_master = (
        int( math.floor((master_bbox[0] - bbox[0]) / res_x_master) ), # x offset
        int( math.floor((bbox[3] - master_bbox[3]) / res_y_master) ), # y offset
        ds_master.RasterXSize, # x size
        ds_master.RasterYSize  # y size
    )

    dst_rect_slave = (
        int( math.floor((slave_bbox[0] - bbox[0]) / res_x_slave) ), # x offset
        int( math.floor((bbox[3] - slave_bbox[3]) / res_y_slave) ), # y offset
        ds_slave.RasterXSize, # x size
        ds_slave.RasterYSize  # y size
    )

    for i in range(1, num_bands+1):
        builder.add_simple_source(i, str(filename_master), i, src_rect=(0, 0, ds_master.RasterXSize, ds_master.RasterYSize), dst_rect=dst_rect_master)
        builder.add_simple_source(num_bands+i , str(filename_slave), i, src_rect=(0, 0, ds_slave.RasterXSize, ds_slave.RasterYSize), dst_rect=dst_rect_slave)
    

    ext = Rect(0,0,size_x, size_y)

    
    pix_master = builder.dataset.GetRasterBand(1).ReadAsArray()
    pix_slave = builder.dataset.GetRasterBand(num_bands +1).ReadAsArray()

    for i in range(2, num_bands+1):
        pix_master = np.dstack((pix_master, builder.dataset.GetRasterBand(i).ReadAsArray()))
        pix_slave = np.dstack((pix_slave, builder.dataset.GetRasterBand(num_bands+i).ReadAsArray()))

    print pix_master.shape, pix_slave.shape

    def _diff(a,b):
        c = np.zeros((a.shape[0],a.shape[1]))
        for i in xrange(a.shape[2]):
            c[:,:] += ( a[:,:,i] - b[:,:,i] )**2
        return np.sqrt(c)

    pix_res = _diff(pix_master, pix_slave)

    max_pix = np.max(pix_res)
    scale = 254.0/max_pix if max_pix > 0 else 1.0

    pix_res = np.array(pix_res*scale+1,'uint8')

    # the output image

    driver_tif = gdal.GetDriverByName('GTiff')
    driver_png = gdal.GetDriverByName('PNG')

    basename = "%s_%s"%( self.identifier,uuid4().hex )
    filename_tif = "/tmp/%s.tif" %( basename )
    filename_png = "/tmp/%s.png" %( basename )

    try:
     
        ds_tif = driver_tif.Create(filename_tif,ext.size_x,ext.size_y,1,gdal.GDT_Byte)
        ds_tif.GetRasterBand(1).WriteArray(pix_res,0,0)

        ds_png = driver_png.CreateCopy( filename_png, ds_tif, 0 )
        
        with open(filename_png) as f:
            output = f.read()

    except Exception as e: 

        if os.path.isfile(filename_tif):
            os.remove(filename_tif)
        if os.path.isfile(filename_png):
            os.remove(filename_png)

        raise e
       
    else:
        os.remove(filename_tif)
        os.remove(filename_png)

    return base64.b64encode(output)
