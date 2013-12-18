#!/usr/bin/env python

from wsgiref import simple_server
from xml.etree import ElementTree
import cgi
import os
      
PORT = 8001
POLICYFILE = "policy.xml"

################################################################################
# AuthServer
################################################################################
class AuthServer(object):

  #------------------------------------------------------------------------------
  # RequestHandler
  #------------------------------------------------------------------------------
  def RequestHandler(self, env, response):
  
      print("\n--------------------------\nIncoming request:")
      status = "200 OK"
      statusNOK = "500 Internal Server Error"
       
      authorizedMessage =   """<?xml version="1.0" encoding="UTF-8"?>
                               <ws:BrwsAuthorizationCheckResponse xsi:schemaLocation="http://ngeo.eo.esa.int/schema/webserver ngEO-EICD-WS.xsd" xmlns:ws="http://ngeo.eo.esa.int/schema/webserver" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                                  <ws:ResponseCode>AUTHORIZED</ws:ResponseCode>
                               </ws:BrwsAuthorizationCheckResponse>"""
  
      deniedMessage =       """<?xml version="1.0" encoding="UTF-8"?>
                               <wsBrwsAuthorizationCheckResponse xsi:schemaLocation="http://ngeo.eo.esa.int/schema/webserver ngEO-EICD-WS.xsd" xmlns:ws="http://ngeo.eo.esa.int/schema/webserver" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                                  <ws:ResponseCode>NOT_AUTHORIZED</ws:ResponseCode>
                               </wsBrwsAuthorizationCheckResponse>"""
  
      try:
        #get the query string parameters from the url
        queryParams = self.GetQueryParams(env)
    
        #check if the user is authorized to access the content based on the query parameters 
        authorized = self.MakePolicyDecision(queryParams)
        
        if authorized:
          print("  -> Request authorized\n")
          responseBody = authorizedMessage
        else:
          print("  -> Request denied\n")
          responseBody = deniedMessage
      except Exception, e:
        print("%s" % str(e))
        responseBody = deniedMessage
        status = statusNOK
      
      response(status, [('Content-type', 'text/xml')])
      
      return ["%s" % responseBody]
      
  
  #------------------------------------------------------------------------------
  # GetQueryParams
  #------------------------------------------------------------------------------
  def GetQueryParams(self, env):
    qs = env.get('QUERY_STRING')
    if qs:
       return dict((k, v)
              for k, v in cgi.parse_qsl(qs))
    else:
       return {}
  
  #------------------------------------------------------------------------------
  # MakePolicyDecision
  #------------------------------------------------------------------------------
  def MakePolicyDecision(self, queryParams):
      authorized = False
      
      browseLayerId = queryParams.get('BrowseLayerId', "")
      userId = queryParams.get('UserId', "")
      
      print("  UserId: [%s]" % userId)
      print("  BrowseLayerId: [%s]" % browseLayerId)
      
      rights = PolicyReader().GetUserRights(userId)
      if browseLayerId in rights:
        authorized = True
      else:
        authorized = False
      
      return authorized

################################################################################
# PolicyReader
################################################################################
class PolicyReader(object):

  def GetUserRights(self, userId):
    rights = []
  
    try:
      #read rights of the user from the policy file
      policyFile = "%s/%s" % (os.path.dirname( __file__ ), POLICYFILE)
      root = ElementTree.parse(policyFile)
      
      for user in root.findall('User'):      
        if (user.attrib["Id"] == userId):
          for right in user.findall('Rights/BrowseLayerId'):
            rights.append(right.text)
            
    except Exception, e:
      print "ERROR while reading policy.xml: %s" % str(e) 
          
    return rights

       
################################################################################
# Main
################################################################################

# Set up the server and bind the request handler function    
server = simple_server.make_server('', PORT, AuthServer().RequestHandler)
print "Authorizer server running on port %d... (Ctrl-C to exit)" % PORT

# Run the server until the process is killed
server.serve_forever()