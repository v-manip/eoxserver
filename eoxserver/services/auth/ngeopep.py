from eoxserver.core import Component, implements 
from eoxserver.services.auth.interfaces import PolicyDecisionPointInterface
from eoxserver.services.auth.exceptions import AuthorisationException
from eoxserver.core.config import get_eoxserver_config

import httplib
from xml.etree import ElementTree
import memcache
import logging
import time
      
def enum(**enums):
  return type('Enum', (), enums)
  
Decision = enum(DONTKNOW=0, AUTHORIZED=1, DENIED=2) 
logger = logging.getLogger(__name__)


################################################################################
# NgeoPEP
################################################################################            
class NgeoPEP(Component):
  implements(PolicyDecisionPointInterface)

  pdp_type = "ngeo"

  #-------------------------------------------------------------------------------
  # authorize
  #-------------------------------------------------------------------------------
  def authorize(self, request):

    try:
      #measure the process    
      timer = AuthTimer()
                
      #Get uid from the request
      userId = request.META['uid']
    
      #TODO: the browseLayerId must be parsed in future by a specialized eoxserver decoder class
      browseLayerId = request.GET.get("BrowseLayer", "empty")
     
      logger.debug("Extracted authorization information: uid=[%s], browse layer id=[%s]" % (userId, browseLayerId))
      
    except Exception, e:
      raise AuthorisationException("Error while extracting information for authorization: %s" % str(e))
   
    return AuthorisationClient().EnforceDecision(userId, browseLayerId)
    
################################################################################
# AuthorisationClient
################################################################################            
class AuthorisationClient(object):  
 
  #-------------------------------------------------------------------------------
  # EnforceDecision
  #-------------------------------------------------------------------------------
  def EnforceDecision(self, userId, browseLayerId):     
      
      decision = Decision.DONTKNOW
      error = ['']  
           
      #See if we find the decision in the cache         
      cachedPD = PolicyDecisionCache().getDecision(userId, browseLayerId)
      if(cachedPD.decision == Decision.DONTKNOW):
        #Ask the server         
        decision = AuthorizerServerInterface().IsRequestAuthorized(userId, browseLayerId, error)
        if not error[0]:
          #Save the decision to our cache
          PolicyDecisionCache().addDecision(userId, browseLayerId, decision)
      else:
        #Apply the cached decision
        decision = cachedPD.decision
      
      if (decision == Decision.AUTHORIZED):
         #OK - let the request through
         logger.debug("User %s is authorized to access browse layer '%s'" % (userId, browseLayerId))
         return True
      else:
         #NOK - deny request
         logger.debug("User %s is NOT authorized to access browse layer '%s'. %s" % (userId, browseLayerId, error[0]))
         raise AuthorisationException("User %s is not authorized to access this content. %s" % (userId, error[0]))
      
################################################################################
# AuthorizerServerInterface
################################################################################    
class AuthorizerServerInterface(object):

  #-------------------------------------------------------------------------------
  # Connect
  #-------------------------------------------------------------------------------
  def Connect(self):
    authServerAddr = ConfigReader().getAuthServerAddress()
    
    #Set up connection to authorizer server and return the connection object 
    return httplib.HTTPConnection(authServerAddr)

     
  #-------------------------------------------------------------------------------
  # IsRequestAuthorized
  #-------------------------------------------------------------------------------
  def IsRequestAuthorized(self, userId, browseLayerId, error):
   
    #Prepare the URL parameters
    url = "/ngEOWebServer/BrwsAuthorizationCheck?UserId=%s&BrowseLayerId=%s" % (userId, browseLayerId)
    
    try:
      conn = self.Connect()

      #Send request
      conn.request("GET", url)
      resp = conn.getresponse()
      
      #Check the response status
      if resp.status != 200:
        raise Exception("server response: %d" % resp.status)
      
      responseBody = resp.read()  
      decision = self.ParseServerResponse(responseBody, error)
            
    except Exception, e:
      decision = Decision.DENIED
      error[0] = "Error while communication with authorizer server: %s" % str(e) 
               
    return decision


#-------------------------------------------------------------------------------
# ParseServerResponse
#-------------------------------------------------------------------------------
  def ParseServerResponse(self, responseBody, error):
  
    #Parse the XML Response and get the elements that we are interested in     
    rootElem = ElementTree.XML(responseBody)    
  
    namespace = '{http://ngeo.eo.esa.int/schema/webserver}'
    responseCodeElem = rootElem.find("%sResponseCode" % namespace)     
    
    if responseCodeElem is None:
      raise Exception("element 'ResponseCode' not found in XML-Response of authorizer server")
      
    #Read the policy decision
    if responseCodeElem.text == "AUTHORIZED":
      decision = Decision.AUTHORIZED
    elif responseCodeElem.text == "NOT_AUTHORIZED":
      decision = Decision.DENIED
    else:
      raise Exception("element 'ResponseCode' does not contain a valid value.")
     
    exceptionElem = rootElem.find("%sExceptionElement" % namespace) 
    if exceptionElem is not None:
      error[0] = exceptionElem.text

    return decision

     
################################################################################
# PolicyDecisionCache
################################################################################    
class PolicyDecisionCache(object):

  cache = None 
  cacheTimeout = 0
  
  def __init__(self):
    self.cacheTimeout = ConfigReader().getCacheTimeout() 
    self.cache = memcache.Client([ConfigReader().getCacheServerAddress()])
     
  #-------------------------------------------------------------------------------
  # addDecision
  #-------------------------------------------------------------------------------  
  def addDecision(self, userId, browseLayerId, decision):
    #Create a new PolicyDecision object
    pd = PolicyDecision()
    pd.userId = userId   
    pd.browseLayerId = browseLayerId 
    pd.decision = decision

    #Create a key for it
    key = self.makeKey(userId, browseLayerId)

    #Save the PolicyDecision in the cache 
    self.cache.set(key, pd, self.cacheTimeout)
                
  #-------------------------------------------------------------------------------
  # getDecision
  #-------------------------------------------------------------------------------  
  def getDecision(self, userId, browseLayerId):
    key = self.makeKey(userId, browseLayerId)
    pd = self.cache.get(key)
    if (pd == None):
      #Return an empty PolicyDecision to signalize that it does not exist in the cache
      pd = PolicyDecision()
      pd.decision = Decision.DONTKNOW
     
    return pd 
    
  #-------------------------------------------------------------------------------
  # makeKey
  #-------------------------------------------------------------------------------  
  def makeKey(self, userId, browseLayerId):
    return ("%s%s" % (userId.encode('utf-8'), browseLayerId.encode('utf-8')))

 
################################################################################
# PolicyDecision
################################################################################    
class PolicyDecision:
  userId = ""
  browseLayerId = ""
  decision = Decision.DONTKNOW
  
  

################################################################################
# ConfigReader
################################################################################
class ConfigReader(object):

  def getAuthServerAddress(self):
      return get_eoxserver_config().get("services.auth.ngeo", "authserver_addr")

  def getCacheServerAddress(self):
      return get_eoxserver_config().get("services.auth.ngeo", "cacheserver_addr")

  def getCacheTimeout(self):
      return get_eoxserver_config().getint("services.auth.ngeo", "cache_timeout")


################################################################################
# AuthTimer
################################################################################
class AuthTimer(object):
  timerStart = 0
  
  def __init__(self):
    self.timerStart = time.time()#time.clock()  
      
  def __del__(self):
    timerEnd = time.time()#time.clock()
    authTime = timerEnd - self.timerStart
    logger.debug("Authorization took %.3f seconds." % (authTime))

