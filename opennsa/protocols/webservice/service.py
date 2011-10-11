"""
Web Service Resource for OpenNSA.

Author: Henrik Thostrup Jensen <htj@nordu.net>
Copyright: NORDUnet (2011)
"""

import datetime
import os
from twisted.python import log

from opennsa import nsa
from opennsa.protocols.webservice.ext import sudsservice

WSDL_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__),"../../../wsdl/"))
WSDL_PROVIDER   = 'file://%s/ogf_nsi_connection_provider_v1_0.wsdl' % WSDL_PATH
WSDL_REQUESTER  = 'file://%s/ogf_nsi_connection_requester_v1_0.wsdl' % WSDL_PATH



def _decodeNSAs(subreq):
    requester_nsa = str(subreq.requesterNSA)
    provider_nsa  = str(subreq.providerNSA)
    return requester_nsa, provider_nsa



class ProviderService:

    def __init__(self, soap_resource, provider):

        self.provider = provider
        self.soap_resource = soap_resource
        #self.nsi_service = nsi_service
        self.decoder = sudsservice.WSDLMarshaller(WSDL_PROVIDER)

        # not sure what query callbacs are doing here
        #self.soap_resource.registerDecoder('"http://schemas.ogf.org/nsi/2011/07/connection/service/queryConfirmed"', ...)
        #self.soap_resource.registerDecoder('"http://schemas.ogf.org/nsi/2011/07/connection/service/queryFailed"', ...)
        self.soap_resource.registerDecoder('"http://schemas.ogf.org/nsi/2011/07/connection/service/reservation"',   self.reservation)
        self.soap_resource.registerDecoder('"http://schemas.ogf.org/nsi/2011/07/connection/service/provision"',     self.provision)
        self.soap_resource.registerDecoder('"http://schemas.ogf.org/nsi/2011/07/connection/service/release"',       self.release)
        self.soap_resource.registerDecoder('"http://schemas.ogf.org/nsi/2011/07/connection/service/terminate"',     self.terminate)
        self.soap_resource.registerDecoder('"http://schemas.ogf.org/nsi/2011/07/connection/service/query"',         self.query)


    def _getGRTParameters(self, grt):
        # GRT = Generic Request Type
        requester_nsa, provider_nsa = _decodeNSAs(grt)
        connection_id               = str(grt.connectionId)
        return requester_nsa, provider_nsa, connection_id


    def _getRequestParameters(self, grt):
        correlation_id  = str(grt.correlationId)
        reply_to        = str(grt.replyTo)
        return correlation_id, reply_to


    def genericReply(connection_id, request, decoder, method, correlation_id):
        reply = decoder.marshal_result(correlation_id, method)
        return reply


    def reservation(self, soap_action, soap_data):

        assert soap_action == '"http://schemas.ogf.org/nsi/2011/07/connection/service/reservation"'
        method, req = self.decoder.parse_request('reservation', soap_data)

        correlation_id, reply_to, = self._getRequestParameters(req)
        res = req.reservation.reservation

        requester_nsa, provider_nsa = _decodeNSAs(req.reservation)
        session_security_attr       = None
        connection_id               = res.connectionId
        global_reservation_id       = res.globalReservationId if 'globalReservationId' in res else None
        description                 = res.description         if 'description'         in res else None
        sp                          = res.serviceParameters
        path                        = res.path

        def parseSTPID(stp_id):
            tokens = stp_id.replace(nsa.STP_PREFIX, '').split(':', 2)
            return nsa.STP(tokens[0], tokens[1])

        source_stp  = parseSTPID(path.sourceSTP.stpId)
        dest_stp    = parseSTPID(path.destSTP.stpId)
        # how to check for existence of optional parameters easily  - in / hasattr both works
        bw = sp.bandwidth
        bwp = nsa.BandwidthParameters(bw.desired if 'desired' in bw else None, bw.minimum if 'minimum' in bw else None, bw.maximum if 'maximum' in bw else None)
        start_time = sp.schedule.startTime
        end_time   = sp.schedule.endTime

        if start_time.tzinfo is None:
            log.msg('No timezone info specified in schedule start time in reservation request, assuming UTC time.')
        st = start_time.utctimetuple()
        start_time = datetime.datetime(st.tm_year, st.tm_mon, st.tm_mday, st.tm_hour, st.tm_min, st.tm_sec)

        if end_time.tzinfo is None:
            log.msg('No timezone info specified in schedule start time in reservation request, assuming UTC time.')
        et = end_time.utctimetuple()
        end_time = datetime.datetime(et.tm_year, et.tm_mon, et.tm_mday, et.tm_hour, et.tm_min, et.tm_sec)

        service_parameters      = nsa.ServiceParameters(start_time, end_time, source_stp, dest_stp, bandwidth=bwp)

        d = self.provider.reservation(correlation_id, reply_to, requester_nsa, provider_nsa, session_security_attr, global_reservation_id, description, connection_id, service_parameters)
        d.addErrback(log.err)

        # The deferred will fire when the reservation is made.

        # The initial reservation ACK should be send when the reservation
        # request is persistent, and a callback should then be issued once
        # the connection has been reserved. Unfortuantely there is
        # currently no way of telling when the request is persitent, so we
        # just return immediately.
        reply = self.decoder.marshal_result(correlation_id, method)
        return reply


    def provision(self, soap_action, soap_data):

        method, req = self.decoder.parse_request('provision', soap_data)

        correlation_id, reply_to, = self._getRequestParameters(req)
        requester_nsa, provider_nsa, connection_id = self._getGRTParameters(req.provision)

        d = self.provider.provision(correlation_id, reply_to, requester_nsa, provider_nsa, None, connection_id)

        reply = self.decoder.marshal_result(correlation_id, method)
        return reply


    def release(self, soap_action, soap_data):

        method, req = self.decoder.parse_request('release', soap_data)

        correlation_id, reply_to, = self._getRequestParameters(req)
        requester_nsa, provider_nsa, connection_id = self._getGRTParameters(req.release)

        d = self.provider.release(correlation_id, reply_to, requester_nsa, provider_nsa, None, connection_id)

        reply = self.decoder.marshal_result(correlation_id, method)
        return reply



    def terminate(self, soap_action, soap_data):

        method, req = self.decoder.parse_request('terminate', soap_data)

        correlation_id, reply_to, = self._getRequestParameters(req)
        requester_nsa, provider_nsa, connection_id = self._getGRTParameters(req.terminate)

        d = self.provider.terminate(correlation_id, reply_to, requester_nsa, provider_nsa, None, connection_id)

        reply = self.decoder.marshal_result(correlation_id, method)
        return reply


    def query(self, soap_action, soap_data):

        method, req = self.decoder.parse_request('query', soap_data)

        requester_nsa, provider_nsa = _decodeNSAs(req.query)
        correlation_id = str(req.correlationId)
        reply_to       = str(req.replyTo)

        operation = req.query.operation
        qf = req.query.queryFilter

        connection_ids = []
        global_reservation_ids = []

        if 'connectionId' in qf:
            connection_ids = qf.connectionId
        if 'globalReservationId' in qf:
            global_reservation_ids = qf.globalReservationId

        d = self.provider.query(correlation_id, reply_to, requester_nsa, provider_nsa, None, operation, connection_ids, global_reservation_ids)

        reply = self.decoder.marshal_result(correlation_id, method)
        return reply



class RequesterService:

    def __init__(self, soap_resource, requester):

        self.soap_resource = soap_resource
        self.requester = requester
        self.decoder = sudsservice.WSDLMarshaller(WSDL_REQUESTER)

        self.soap_resource.registerDecoder('"http://schemas.ogf.org/nsi/2011/07/connection/service/reservationConfirmed"',  self.reservationConfirmed)
        self.soap_resource.registerDecoder('"http://schemas.ogf.org/nsi/2011/07/connection/service/reservationFailed"',     self.reservationFailed)

        self.soap_resource.registerDecoder('"http://schemas.ogf.org/nsi/2011/07/connection/service/provisionConfirmed"',    self.provisionConfirmed)
        self.soap_resource.registerDecoder('"http://schemas.ogf.org/nsi/2011/07/connection/service/provisionFailed"',       self.provisionFailed)

        self.soap_resource.registerDecoder('"http://schemas.ogf.org/nsi/2011/07/connection/service/releaseConfirmed"',      self.releaseConfirmed)
        self.soap_resource.registerDecoder('"http://schemas.ogf.org/nsi/2011/07/connection/service/releaseFailed"',         self.releaseFailed)

        self.soap_resource.registerDecoder('"http://schemas.ogf.org/nsi/2011/07/connection/service/terminateConfirmed"',    self.terminateConfirmed)
        self.soap_resource.registerDecoder('"http://schemas.ogf.org/nsi/2011/07/connection/service/terminateFailed"',       self.terminateFailed)

        self.soap_resource.registerDecoder('"http://schemas.ogf.org/nsi/2011/07/connection/service/queryConfirmed"',        self.queryConfirmed)
        self.soap_resource.registerDecoder('"http://schemas.ogf.org/nsi/2011/07/connection/service/queryFailed"',           self.queryFailed)

        #"http://schemas.ogf.org/nsi/2011/07/connection/service/forcedEnd"
        #"http://schemas.ogf.org/nsi/2011/07/connection/service/query"


    def _getGFTParameters(self, gft):
        # GFT = GenericFailedType

        requester_nsa, provider_nsa = _decodeNSAs(gft)
        global_reservation_id       = str(gft.globalReservationId) if 'globalReservationId' in gft else None
        connection_id               = str(gft.connectionId)
        connection_state            = str(gft.connectionState)
        error_message               = None
        if 'ServiceException' in gft:
            error_message           = str(gft.ServiceException.text)

        return requester_nsa, provider_nsa, global_reservation_id, connection_id, connection_state, error_message


    def reservationConfirmed(self, soap_action, soap_data):

        assert soap_action == '"http://schemas.ogf.org/nsi/2011/07/connection/service/reservationConfirmed"'
        method, req = self.decoder.parse_request('reservationConfirmed', soap_data)

        requester_nsa, provider_nsa = _decodeNSAs(req.reservationConfirmed)
        res = req.reservationConfirmed.reservation

        correlation_id          = str(req.correlationId)
        global_reservation_id   = str(res.globalReservationId)
        description             = str(res.description)
        connection_id           = str(res.connectionId)

        self.requester.reservationConfirmed(correlation_id, requester_nsa, provider_nsa, None, global_reservation_id, description, connection_id, None)

        reply = self.decoder.marshal_result(correlation_id, method)
        return reply


    def reservationFailed(self, soap_action, soap_data):

        assert soap_action == '"http://schemas.ogf.org/nsi/2011/07/connection/service/reservationFailed"'
        method, req = self.decoder.parse_request('reservationFailed', soap_data)

        correlation_id = str(req.correlationId)
        requester_nsa, provider_nsa, global_reservation_id, connection_id, connection_state, error_message = self._getGFTParameters(req.reservationFailed)

        self.requester.reservationFailed(correlation_id, requester_nsa, provider_nsa, global_reservation_id, connection_id, connection_state, error_message)

        reply = self.decoder.marshal_result(correlation_id, method)
        return reply


    def provisionConfirmed(self, soap_action, soap_data):

        assert soap_action == '"http://schemas.ogf.org/nsi/2011/07/connection/service/provisionConfirmed"'
        method, req = self.decoder.parse_request('provisionConfirmed', soap_data)

        requester_nsa, provider_nsa = _decodeNSAs(req.provisionConfirmed)
        correlation_id  = str(req.correlationId)
        connection_id   = str(req.provisionConfirmed.connectionId)

        d = self.requester.provisionConfirmed(correlation_id, requester_nsa, provider_nsa, None, connection_id)

        reply = self.decoder.marshal_result(correlation_id, method)
        return reply


    def provisionFailed(self, soap_action, soap_data):

        assert soap_action == '"http://schemas.ogf.org/nsi/2011/07/connection/service/provisionFailed"'
        method, req = self.decoder.parse_request('provisionFailed', soap_data)

        correlation_id = str(req.correlationId)
        requester_nsa, provider_nsa, global_reservation_id, connection_id, connection_state, error_message = self._getGFTParameters(req.provisionFailed)

        d = self.requester.provisionFailed(correlation_id, requester_nsa, provider_nsa, global_reservation_id, connection_id, connection_state, error_message)

        reply = self.decoder.marshal_result(correlation_id, method)
        return reply


    def releaseConfirmed(self, soap_action, soap_data):

        assert soap_action == '"http://schemas.ogf.org/nsi/2011/07/connection/service/releaseConfirmed"'
        method, req = self.decoder.parse_request('releaseConfirmed', soap_data)

        requester_nsa, provider_nsa = _decodeNSAs(req.releaseConfirmed)
        correlation_id  = str(req.correlationId)
        connection_id   = str(req.releaseConfirmed.connectionId)

        d = self.requester.releaseConfirmed(correlation_id, requester_nsa, provider_nsa, None, connection_id)

        reply = self.decoder.marshal_result(correlation_id, method)
        return reply


    def releaseFailed(self, soap_action, soap_data):

        assert soap_action == '"http://schemas.ogf.org/nsi/2011/07/connection/service/releaseFailed"'
        method, req = self.decoder.parse_request('releaseFailed', soap_data)

        correlation_id = str(req.correlationId)
        requester_nsa, provider_nsa, global_reservation_id, connection_id, connection_state, error_message = self._getGFTParameters(req.releaseFailed)

        d = self.requester.releaseFailed(correlation_id, requester_nsa, provider_nsa, global_reservation_id, connection_id, connection_state, error_message)

        reply = self.decoder.marshal_result(correlation_id, method)
        return reply


    def terminateConfirmed(self, soap_action, soap_data):

        assert soap_action == '"http://schemas.ogf.org/nsi/2011/07/connection/service/terminateConfirmed"'
        method, req = self.decoder.parse_request('terminateConfirmed', soap_data)

        requester_nsa, provider_nsa = _decodeNSAs(req.terminateConfirmed)
        correlation_id  = str(req.correlationId)
        connection_id   = str(req.terminateConfirmed.connectionId)

        d = self.requester.terminateConfirmed(correlation_id, requester_nsa, provider_nsa, None, connection_id)

        reply = self.decoder.marshal_result(correlation_id, method)
        return reply


    def terminateFailed(self, soap_action, soap_data):

        assert soap_action == '"http://schemas.ogf.org/nsi/2011/07/connection/service/terminateFailed"'
        method, req = self.decoder.parse_request('terminateFailed', soap_data)

        correlation_id = str(req.correlationId)
        requester_nsa, provider_nsa, global_reservation_id, connection_id, connection_state, error_message = self._getGFTParameters(req.terminateFailed)

        d = self.requester.terminateFailed(correlation_id, requester_nsa, provider_nsa, global_reservation_id, connection_id, connection_state, error_message)

        reply = self.decoder.marshal_result(correlation_id, method)
        return reply


    def queryConfirmed(self, soap_action, soap_data):

        assert soap_action == '"http://schemas.ogf.org/nsi/2011/07/connection/service/queryConfirmed"'
        method, req = self.decoder.parse_request('queryConfirmed', soap_data)

        requester_nsa, provider_nsa = _decodeNSAs(req.queryConfirmed)

        correlation_id          = str(req.correlationId)
        #reservation_summary     = req.queryConfirmed
        #connection_id           = str(req.terminateConfirmed.connectionId)

        query_result = req # should really translate this to something generic

        d = self.requester.queryConfirmed(correlation_id, requester_nsa, provider_nsa, query_result)

        reply = self.decoder.marshal_result(correlation_id, method)
        return reply


    def queryFailed(self, soap_action, soap_data):
        raise NotImplementedError('Service query failure handling not implemented in Requestor.')

