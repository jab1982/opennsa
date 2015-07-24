"""
  OESS Backend

  Created by Jeronimo Bezerra/AmLight - jab@amlight.net
  Enhanced by AJ Ragusa GlobalNOC - aragusa@grnoc.iu.edu
  Comments by Henrik Jensen - htj@nordu.net

"""
from twisted.python import log
from twisted.internet import defer

from opennsa.backends.common import genericbackend
from opennsa import constants as cnt, config
from opennsa import error

import string
import random

import urllib2
import json

import logging

def oess_get_wg_id(url, wg):
    """ Get WG_ID using Workgroup on opennsa.conf
        Reuses http session created by oess_authenticate function
    """

    query = 'services/data.cgi?action=get_workgroups'
    tmp = urllib2.urlopen(url + query )

    # Extract the WG_ID from the json output and return it
    jsonData = json.loads(tmp.read())
    searchResults = jsonData['results']
    for er in searchResults:
       if er['name'] == wg:
         wg_id = er['workgroup_id']
         # Debug
         log.msg('OESS: oess_get_wg_id, workgroup_id: %s' % (wg_id), logLevel = logging.DEBUG)
         return wg_id

    log.msg('OESS: unable to find workgroup named: %s' % (wg), logLevel = logging.ERROR)


def oess_authenticate(url, user, pw, wg, log_system):
    """ Authenticate against OESS using HTTPBasicAuth
        Creates a opener for future queries
    """
    # Debug
    log.msg('OESS: oess_authenticate and url: %s' % (url), logLevel=logging.DEBUG)

    # create a password manager
    password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()

    try:
       # Add the username and password.
       password_mgr.add_password(None, url, user, pw)
       handler = urllib2.HTTPBasicAuthHandler(password_mgr)

       # create "opener" (OpenerDirector instance)
       opener = urllib2.build_opener(handler)

       # use the opener to fetch a URL
       opener.open(url)

       # Install the opener.
       # Now all calls to urllib2.urlopen use our opener.
       urllib2.install_opener(opener)

       # Return the wg_id
       wg_id = oess_get_wg_id(url, wg)
       return wg_id
    except:
       log.msg('ERROR: User or Password Incorrect.', logLevel = logging.ERROR)
       log.err()
       return defer.fail(error.InternalNRMError('OESS\'s User or Password Incorrect'))

def oess_validate_interface(url, interface):
    #parse the interface/switch/vlan tag
    (sw,int_vlan) = interface.split(':')
    (iface,vlan) = int_vlan.split('#')

    #request the data about the interface
    query, action = 'services/data.cgi?', 'action=get_node_interfaces&node='
    data = urllib2.urlopen(url + query + action + sw)
    jsonData = json.loads(data.read())
    searchResults = jsonData['results']
    if_exist = 0
    for res in searchResults:
        if res['name'] == iface:
            if_exist = 1
    if if_exist == 0:
        return defer.fail(error.InternalNRMError('ERROR: Configured interface %s does not exist' % iface))

    #verify the interface
    query = 'services/data.cgi?action=is_vlan_tag_available&node=%s&vlan=%s&interface=%s' % (sw,vlan,iface)
    data = urllib2.urlopen(url + query)
    jsonData = json.loads(data.read())
    searchResults = jsonData['results']
    if searchResults:
        # Debug
        # print searchResults[0]
        if searchResults[0]['available'] == 0:
            return defer.fail(error.InternalNRMError('ERROR: VLAN %s not available on interface %s' % (vlan, iface)))
    else:
        return defer.fail(error.InternalNRMError('ERROR: interface %s does not exist on switch %s' % (iface, sw)))

    return sw, iface, vlan

def oess_validate_input(url, input1, input2):
    """ Validate switches, interfaces and VLANs chosen
        In case of error, return 99 for switch issues, 98 for vlan/interfaces issues and 93 for interfaces issues
        Input should be: sw:interface#vlan
    """

    (sw1, if1, vlan1) = oess_validate_interface(url, input1)
    (sw2, if2, vlan2) = oess_validate_interface(url, input2)


    return sw1, sw2, if1, if2, vlan1, vlan2
    # If we get here, switches, interfaces and vlans are correct. Now, provision the circuit

def find_path(url, sw1, sw2, used_links):
    # Find primary path

    query = 'services/data.cgi?action=get_shortest_path&node=%s&node=%s' % (sw1, sw2)

    for link in used_links:
        query += "&link=%s" % (link)

    data = urllib2.urlopen(url + query)
    jsonData = json.loads(data.read())
    searchResults = jsonData['results']
    path = []
    if searchResults:
        for link in searchResults:
           path.append(link['link'])
    else:
        return defer.fail(error.InternalNRMError('ERROR: There is no path between %s and %s' % (sw1, sw2)))

    return path

def oess_provision_circuit(url, wg_id ,sw1, sw2, if1, if2, vlan1, vlan2):
    """ Provision the circuit using parameters received
        Support Backup and Primary path
    """
    # Debug
    log.msg('OESS: oess_provision_circuit', logLevel = logging.DEBUG)

    provision_string = 'action=provision_circuit&workgroup_id=%s&node=%s&interface=%s&tag=%s&node=%s&interface=%s&tag=%s' % (wg_id, sw1, if1, vlan1, sw2, if2, vlan2)

    primary_path = find_path(url, sw1, sw2, [])
    backup_path = find_path(url, sw1, sw2, primary_path)

    for link in primary_path:
        provision_string += "&link=" + link

    for link in backup_path:
        provision_string += "&backup_link=" + link

    #start/end times
    provision_string += '&provision_time=-1&remove_time=-1&description=NSI-VLAN-%s' % (vlan1)

    # start and remove time are handled by OpenNSA, so we set as -1
    query = 'services/provisioning.cgi?' + provision_string
    request = urllib2.urlopen(url + query)
    jsonData = json.loads(request.read())

    searchResults = jsonData['results']
    if searchResults['success']:
        return searchResults['circuit_id']
    else:
        return defer.fail(error.InternalNRMError('ERROR: It was not possible to provision the circuit. Check OESS logs'))

def oess_remove_circuit(url, circuit_id, wg_id):
    """ Remove circuit using circuit_id and workgroup_id

    """
    # Debug
    log.msg('OESS: oess_remove_circuit')

    action = 'services/provisioning.cgi?action=remove_circuit&circuit_id=%s&remove_time=-1&workgroup_id=%s' % (circuit_id, wg_id)
    request = urllib2.urlopen(url + action)
    jsonData = json.loads(request.read())
    searchResults = jsonData['results']
    if jsonData:
        pass
        log.msg('OESS: oess_remove_circuit: circuit %s removed' % circuit_id, logLevel = logging.INFO)
    else:
        return defer.fail(error.InternalNRMError('ERROR: it was not possible to remove the circuit'))


def OESSBackend(network_name, nrm_ports, parent_requester, configuration):
    log.msg('OESS: OESSBackend')
    name = 'OESS NRM %s' % network_name
    nrm_map  = dict( [ (p.name, p) for p in nrm_ports ] ) # for the generic backend
    port_map = dict( [ (p.name, p.interface) for p in nrm_ports ] ) # for the nrm backend

    cm = OESSConnectionManager(name, port_map, configuration)
    return genericbackend.GenericBackend(network_name, nrm_map, cm, parent_requester, name, minimum_duration=1)

class OESSConnectionManager:

    def __init__(self, log_system, port_map, cfg):
        self.log_system = log_system
        self.port_map   = port_map

        self.url = cfg[config.OESS_URL]
        self.username = cfg[config.OESS_USER]
        self.password = cfg[config.OESS_PASSWORD]
        self.workgroup = cfg[config.OESS_WORKGROUP]

        self.wg_id = oess_authenticate(self.url, self.username, self.password, self.workgroup, log_system)


    def getResource(self, port, label_type, label_value):
        # Debug
        log.msg('OESS: getResource',system=self.log_system)
        return self.port_map[port] + ':' + str(label_value)


    def getTarget(self, port, label_type, label_value):
        # Debug
        log.msg('OESS: getTarget, port_map[port] = %s and str(label_value) = %s' % (self.port_map[port], str(label_value)),system=self.log_system)
        return self.port_map[port] + '#' + str(label_value)


    def createConnectionId(self, source_target, dest_target):
        # Debug
        log.msg('OESS: createConnectionId',system=self.log_system)
        return 'OESS-' + ''.join( [ random.choice(string.hexdigits[:16]) for _ in range(8) ] )


    def canSwapLabel(self, label_type):
        # Debug
        log.msg('OESS: canSwapLabel',system=self.log_system)
        #return True
        return False


    def setupLink(self, connection_id, source_target, dest_target, bandwidth):
        # Debug
        log.msg('OESS: setupLink',system=self.log_system)
        sw1, sw2, if1, if2, vlan1, vlan2 = oess_validate_input(self.url, source_target, dest_target)
        self.circuit_id = oess_provision_circuit(self.url, self.wg_id, sw1, sw2, if1, if2, vlan1, vlan2)
        log.msg('Link %s -> %s up, circuit_id = %s' % (source_target, dest_target, self.circuit_id), system=self.log_system)
        return defer.succeed(None)


    def teardownLink(self, connection_id, source_target, dest_target, bandwidth):
        # Debug
        log.msg('OESS: teardownLink and self.circuit_id = %s' % self.circuit_id, system=self.log_system)
        oess_remove_circuit(self.url, self.circuit_id, self.wg_id)
        log.msg('Link %s -> %s down' % (source_target, dest_target), system=self.log_system)
        return defer.succeed(None)
