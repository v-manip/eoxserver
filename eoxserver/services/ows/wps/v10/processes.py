


from datetime import datetime
import struct

from django.contrib.gis.geos import Point
from django.contrib.gis.gdal import SpatialReference

from eoxserver.core import Component, ExtensionPoint, implements
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
        "x": float,
        "y": float,
        "srid": int
    }

    outputs = {
        "processed": str
    }

    def execute(self, collection, begin_time, end_time, x, y, srid):
        """ The main execution function for the process.
        """

        # parameter parsing
        point = Point(x, y)
        point.srid = srid


        collection = models.Collection.objects.get(identifier=collection)

        print collection, begin_time, end_time

        eo_objects = collection.eo_objects.filter(
            footprint__intersects=point
        ).exclude(
            begin_time__gt=end_time, end_time__lt=begin_time
        )

        print eo_objects

        for eo_object in eo_objects:
            coverage = eo_object.cast()
            for data_item in coverage.data_items.filter(semantic__startswith="bands"):
                filename = connect(data_item)
                ds = gdal.Open(filename)
                sr = SpatialReference(ds.GetProjection())

                point.transform(sr)

                gt = ds.GetGeoTransform()

                px = int((point[0] - gt[0]) / gt[1]) #x pixel
                py = int((point[1] - gt[3]) / gt[5]) #y pixel

                #array = ds.ReadRaster(px, py, 1, 1)
                structval = ds.ReadRaster(px,py,1,1,buf_type=gdal.GDT_Byte) #TODO: Check Range Type to adapt buf_type!
                print structval
                #pixel_value = array[0][0]
                pixel_value = struct.unpack('BBBB' , structval) #use the 'short' format code (2 bytes) not int (4 bytes)
                print pixel_value




        return {
            "processed": "Test output %s, %s" % (x, y)
        }