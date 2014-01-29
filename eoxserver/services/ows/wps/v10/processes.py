


from eoxserver.core import Component, ExtensionPoint, implements
from eoxserver.services.ows.wps.interfaces import ProcessInterface
from datetime import datetime
from eoxserver.services.ows.wps.parameters import LiteralData, ComplexData



class RandomProcess(Component):
    implements(ProcessInterface)

    identifier = "random"
    title = "Random Number Generator"
    description = "Creates a string output in csv style with the defined number of random generated values."
    metadata = ["a", "b"]
    profiles = ["p", "q"]

    inputs = {
        "X": int,
        "Y": int
    }

    outputs = {
        "processed": str
    }

    def execute(self, X, Y):
        """ The main execution function for the process.
        """
        return {
            "processed": "Test output " + X + ", " + Y       
        }