#-------------------------------------------------------------------------------
# $Id$
#
# Project: EOxServer <http://eoxserver.org>
# Authors: Fabian Schindler <fabian.schindler@eox.at>
#
#-------------------------------------------------------------------------------
# Copyright (C) 2011 EOX IT Services GmbH
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


from eoxserver.core import Component, implements
from eoxserver.core.decoders import xml, kvp, typelist, lower
from eoxserver.services.ows.interfaces import (
    ServiceHandlerInterface, GetServiceHandlerInterface, 
    PostServiceHandlerInterface, VersionNegotiationInterface
)
from eoxserver.services.ows.wcs.basehandlers import (
    WCSGetCapabilitiesHandlerBase
)
from eoxserver.services.ows.wcs.parameters import WCSCapabilitiesRenderParams
from eoxserver.services.ows.wcs.v10.util import nsmap


class WCS10GetCapabilitiesHandler(WCSGetCapabilitiesHandlerBase, Component):
    implements(ServiceHandlerInterface)
    implements(GetServiceHandlerInterface)
    implements(PostServiceHandlerInterface)
    implements(VersionNegotiationInterface)

    versions = ("1.0.0",)

    def get_decoder(self, request):
        if request.method == "GET":
            return WCS10GetCapabilitiesKVPDecoder(request.GET)
        elif request.method == "POST":
            return WCS10GetCapabilitiesXMLDecoder(request.body)

    def get_params(self, coverages, decoder):
        return WCSCapabilitiesRenderParams(
            coverages, "1.0.0", decoder.sections, decoder.acceptlanguages, 
            decoder.acceptformats, decoder.updatesequence
        )


class WCS10GetCapabilitiesKVPDecoder(kvp.Decoder):
    sections            = kvp.Parameter(type=typelist(lower, ","), num="?", default=["all"])
    updatesequence      = kvp.Parameter(num="?")
    acceptversions      = kvp.Parameter(type=typelist(str, ","), num="?")
    acceptformats       = kvp.Parameter(type=typelist(str, ","), num="?", default=["text/xml"])
    acceptlanguages     = kvp.Parameter(type=typelist(str, ","), num="?")


class WCS10GetCapabilitiesXMLDecoder(xml.Decoder):
    sections            = xml.Parameter("/ows:Sections/ows:Section/text()", num="*")
    updatesequence      = xml.Parameter("/@updateSequence", num="?")
    acceptversions      = xml.Parameter("/ows:AcceptVersions/ows:Version/text()", num="*")
    acceptformats       = xml.Parameter("/ows:AcceptFormats/ows:OutputFormat/text()", num="*", default=["text/xml"])
    acceptlanguages     = xml.Parameter("/ows:AcceptLanguages/ows:Language/text()", num="*")

    namespaces = nsmap
