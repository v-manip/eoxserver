#-------------------------------------------------------------------------------
# $Id$
#
# Project: EOxServer <http://eoxserver.org>
# Authors: Fabian Schindler <fabian.schindler@eox.at>
#
#-------------------------------------------------------------------------------
# Copyright (C) 2013 EOX IT Services GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
# copies of the Software, and to permit persons to whom the Software is 
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies of this Software or works derived from this Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#-------------------------------------------------------------------------------


import logging

from eoxserver.core.config import get_eoxserver_config
from eoxserver.core.decoders import config, typelist
from eoxserver.contrib import mapserver as ms
from eoxserver.services.mapserver.wms.layerfactories import (
    BaseStyleMixIn, AbstractLayerFactory
)


logger = logging.getLogger(__name__)


class ColorizedMaskLayerFactory(BaseStyleMixIn, AbstractLayerFactory):

    @property
    def suffixes(self):
        return ["_%s" % mask for mask in self.enabled_masks]

    @property
    def enabled_masks(self):
        decoder = EnabledMasksConfigReader(get_eoxserver_config())
        return decoder.mask_names

    
    def generate(self, eo_object, group_layer, suffix, options):
        mask_name = suffix[1:]
        coverage = eo_object.cast()
        
        if mask_name not in self.enabled_masks:
            return

        mask_items = coverage.data_items.filter(
            semantic="polygonmask[%s]" % mask_name
        )

        if not group_layer and len(mask_items) > 1:
            group_layer = self.generate_group(coverage.identifier + suffix)
            yield group_layer, ()


        for i, mask_item in enumerate(mask_items):
            # TODO: datelinewrap?
            if group_layer:
                name = "%s%s_%d" % (coverage.identifier, suffix, i)
            else:
                name = "%s%s" % (coverage.identifier, suffix)

            layer = self._create_layer(coverage,
                name, None, #coverage.extent
            )
            self._set_projection(layer, coverage.spatial_reference)
            
            layer.type = ms.MS_LAYER_POLYGON
            layer.dump = True

            if group_layer:
                c = group_layer.connection
                if not c:
                    group_layer.connection = layer.name
                else:
                    group_layer.connection = "%s,%s" % (c, layer.name)
            else:
                self.apply_styles(layer, fill=True)

            yield layer, (mask_item,)


    def generate_group(self, name):
        layer = ms.layerObj()
        layer.name = name
        layer.type = ms.MS_LAYER_POLYGON
        layer.connectiontype = ms.MS_UNION
        layer.connection = ""
        self.apply_styles(layer, fill=True)
        return layer


class EnabledMasksConfigReader(config.Reader):
    section = "services.ows.wms"
    mask_names = config.Option(type=typelist(str, ","), default=[])
