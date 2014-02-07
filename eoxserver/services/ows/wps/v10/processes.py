


from datetime import datetime
import struct
import csv
from StringIO import StringIO


from django.contrib.gis.geos import Point, MultiPoint
from django.contrib.gis.gdal import SpatialReference

from eoxserver.core import Component, ExtensionPoint, implements
from eoxserver.core.util.timetools import isoformat
from eoxserver.contrib import gdal
from eoxserver.backends.access import connect
from eoxserver.services.ows.wps.interfaces import ProcessInterface
from eoxserver.services.ows.wps.parameters import LiteralData, ComplexData
from eoxserver.resources.coverages import models



class RandomProcess(Component):
    implements(ProcessInterface)

    identifier = "getdata"
    title = "Retrieves value at requested point"
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
        eo_objects = collection.eo_objects.exclude(
            begin_time__gt=end_time, end_time__lt=begin_time
        )

        coordinates = coord_list.split(';')
        print coordinates

        points = []
        for coordinate in coordinates:
            x,y = coordinate.split(',')
            # parameter parsing
            point = Point(float(x), float(y))
            point.srid = srid
            points.append(point)

        points = MultiPoint(points)
        points.srid = srid

        print collection, begin_time, end_time

        eo_objects = eo_objects.filter(
            footprint__intersects=points
        )

        print eo_objects

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
            print data_item
            filename = connect(data_item)
            ds = gdal.Open(filename)
            sr = SpatialReference(ds.GetProjection())
            #points_t = points.transform(sr, clone=True)

            for index, point in enumerate(points, start=1):

                if not coverage.footprint.contains(point):
                    continue

                print "Point in coverage"
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