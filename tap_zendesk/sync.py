import json
from zenpy.lib.api_objects import BaseObject
from zenpy.lib.proxy import ProxyList

import singer
import singer.metrics as metrics
from singer import metadata
from singer import Transformer

LOGGER = singer.get_logger()


def listify_ticket_audit_event_values(record: dict) -> dict:
    """Convert string event values to list to ensure a consistent data type across rows"""
    for evt in record.get("events"):
        if isinstance(evt.get("value"), str):
            evt["value"] = [evt["value"]]
    return record


def process_record(record, stream_id: str):
    """ Serializes Zenpy's internal classes into Python objects via ZendeskEncoder. """
    rec_str = json.dumps(record, cls=ZendeskEncoder)
    rec_dict = json.loads(rec_str)
    if stream_id == "ticket_audits":
        rec_dict = listify_ticket_audit_event_values(rec_dict)
    return rec_dict


def sync_stream(state, start_date, instance):
    stream = instance.stream

    # If we have a bookmark, use it; otherwise use start_date
    if (instance.replication_method == 'INCREMENTAL' and
            not state.get('bookmarks', {}).get(stream.tap_stream_id, {}).get(instance.replication_key)):
        singer.write_bookmark(state,
                              stream.tap_stream_id,
                              instance.replication_key,
                              start_date)

    parent_stream = stream
    with metrics.record_counter(stream.tap_stream_id) as counter, Transformer() as transformer:
        for (stream, record) in instance.sync(state):
            # NB: Only count parent records in the case of sub-streams
            if stream.tap_stream_id == parent_stream.tap_stream_id:
                counter.increment()

            rec = process_record(record, stream.tap_stream_id)
            # SCHEMA_GEN: Comment out transform
            rec = transformer.transform(rec, stream.schema.to_dict(), metadata.to_map(stream.metadata))

            singer.write_record(stream.tap_stream_id, rec)
            # NB: We will only write state at the end of a stream's sync:
            #  We may find out that there exists a sync that takes too long and can never emit a bookmark
            #  but we don't know if we can guarentee the order of emitted records.

        if instance.replication_method == "INCREMENTAL":
            singer.write_state(state)

        return counter.value


class ZendeskEncoder(json.JSONEncoder):
    def default(self, obj):  # pylint: disable=arguments-differ,method-hidden
        if isinstance(obj, BaseObject):
            obj_dict = obj.to_dict()
            for k, v in list(obj_dict.items()):
                # NB: This might fail if the object inside is callable
                if callable(v):
                    obj_dict.pop(k)
            return obj_dict
        elif isinstance(obj, ProxyList):
            return obj.copy()
        return json.JSONEncoder.default(self, obj)
