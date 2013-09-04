'''
Score datasets on Sir Tim Berners-Lee\'s five stars of openness
based on mime-type.
'''
import datetime
import mimetypes
import json
import requests
import urlparse
import ckan.lib.celery_app as celery_app
from ckanext.archiver.tasks import link_checker, LinkCheckerError


class QAError(Exception):
    pass


class CkanError(Exception):
    pass


OPENNESS_SCORE_REASON = {
    -1: 'unrecognised content type',
    0: 'not obtainable',
    1: 'obtainable via web page',
    2: 'machine readable format',
    3: 'open and standardized format',
    4: 'ontologically represented',
    5: 'fully Linked Open Data as appropriate',
}

ALLOWED_MIME_TYPES = {
    'atom': [ 'application/atom+xml' ],
    'rss': [ 'application/rss+xml' ],
    'csv': [ 'text/csv' ],
    'json': [ 'application/json' ],
    'json-ld': [ 'application/json', 'application/ld+json' ],
    'kml': [ 'application/vnd.google-earth.kml+xml' ],
    'turtle': [ 'text/turtle', 'application/x-turtle' ],
    'n-triples': [ 'text/plain'],
    'n3': [ 'text/rdf+n3', 'text/plain', ],
    'rdf+xml': [ 'application/rdf+xml' ],
    'tsv': [ 'text/tab-separated-values' ],
    'api/sparql': [ 'application/sparql-query' ],
    'html': [ 'text/html', 'application/xhtml+xml' ],
    'txt': [ 'text/plain' ],
    'rtf': [ 'text/rtf', 'application/rtf' ],
    'pdf': [ 'application/pdf' ],
    'ps': [ 'application/postscript' ],
    'tei': [ 'application/tei+xml' ],
    'tei+zip': [ 'application/zip', 'application/x-gzip', 'application/x-gtar', 'application/x-tar' ],
    'snapshot': [ 'text/html' ],
    'iso': [ 'application/octet-stream' ],
    'svg': [ 'image/svg+xml' ],
    'gedcom': [ 'application/octet-stream', 'text/xml' ],
}

MIME_TYPE_SCORE = {
    'atom': 2,
    'application/atom+xml': 2,
    'snapshot': 1,
    'iso': 1,
    'application/octet-stream': 1,
    'rss': 2,
    'application/rss+xml': 2,
    'ore': 3,
    'csv': 3,
    'text/csv': 3,
    'json': 3,
    'application/json': 3,
    'json-ld': 4,
    'owl+xml': 4,
    'kml': 3,
    'application/vnd.google-earth.kml+xml': 3,
    'rdf+json': 4,
    'n3': 4,
    'text/n3': 4,
    'n-triples': 4,
    'turtle': 4,
    'text/turtle': 4,
    'rdf+xml': 4,
    'application/rdf+xml': 4,
    'svg': 3,
    'text/svg+xml': 3,
    'tsv': 3,
    'text/tab-separated-values': 3,
    'htm': 1,
    'mei': 3,
    'api': 2,
    'api/rest': 3,
    'api/sparql': 3,
    'application/sparql-query': 3,
    'html': 2,
    'application/xml': 3,
    'text/xml': 3,
    'text/html': 1,
    'application/xhtml+xml': 1,
    'text/plain': 1,
    'text/rtf': 1,
    'application/rtf': 1,
    'txt': 1,
    'application/pdf': 1,
    'application/postscript': 1,
    'pdf': 1,
    'ps': 1,
    'tei': 3,
    'application/tei+xml': 3,
    'tei+zip': 3,
    'application/zip': 1,
    'application/x-gzip': 1,
    'application/x-gtar': 1,
    'application/x-tar': 1,
    'gedcom': 3,
}


def _update_task_status(context, data):
    """
    Use CKAN API to update the task status. The data parameter
    should be a dict representing one row in the task_status table.

    Returns the content of the response.
    """
    api_url = urlparse.urljoin(context['site_url'], 'api/action')
    res = requests.post(
        api_url + '/task_status_update', json.dumps(data),
        headers={'Authorization': context['apikey'],
                 'Content-Type': 'application/json'}
    )
    if res.status_code == 200:
        return res.content
    else:
        raise CkanError('ckan failed to update task_status, status_code (%s), error %s'
                        % (res.status_code, res.content))


def _task_status_data(id, result):
    return [
        {
            'entity_id': id,
            'entity_type': u'resource',
            'task_type': 'qa',
            'key': u'openness_score',
            'value': result['openness_score'],
            'last_updated': datetime.datetime.now().isoformat()
        },
        {
            'entity_id': id,
            'entity_type': u'resource',
            'task_type': 'qa',
            'key': u'openness_score_reason',
            'value': result['openness_score_reason'],
            'last_updated': datetime.datetime.now().isoformat()
        },
        {
            'entity_id': id,
            'entity_type': u'resource',
            'task_type': 'qa',
            'key': u'openness_score_failure_count',
            'value': result['openness_score_failure_count'],
            'last_updated': datetime.datetime.now().isoformat()
        },
    ]


@celery_app.celery.task(name="qa.update")
def update(context, data):
    """
    Score resources on Sir Tim Berners-Lee\'s five stars of openness
    based on mime-type.
    
    Returns a JSON dict with keys:

        'openness_score': score (int)
        'openness_score_reason': the reason for the score (string)
        'openness_score_failure_count': the number of consecutive times that
                                        this resource has returned a score of 0
    """
    log = update.get_logger()
    try:
        data = json.loads(data)
        context = json.loads(context)

        result = resource_score(context, data)
        log.info('Openness score for dataset %s (res#%s): %r (%s)',
                 data['package'], data['position'],
                 result['openness_score'], result['openness_score_reason'])
        
        task_status_data = _task_status_data(data['id'], result)

        api_url = urlparse.urljoin(context['site_url'], 'api/action')
        response = requests.post(
            api_url + '/task_status_update_many',
            json.dumps({'data': task_status_data}),
            headers={'Authorization': context['apikey'],
                     'Content-Type': 'application/json'}
        )
        if not response.ok:
            err = 'ckan failed to update task_status, error %s' \
                  % response.error
            log.error(err)
            raise CkanError(err)
        elif response.status_code != 200:
            err = 'ckan failed to update task_status, status_code (%s), error %s' \
                  % (response.status_code, response.content)
            log.error(err)
            raise CkanError(err)

        return json.dumps(result)
    except Exception, e:
        log.error('Exception occurred during QA update: %s: %s', e.__class__.__name__,  unicode(e))
        _update_task_status(context, {
            'entity_id': data['id'],
            'entity_type': u'resource',
            'task_type': 'qa',
            'key': u'celery_task_id',
            'value': unicode(update.request.id),
            'error': '%s: %s' % (e.__class__.__name__,  unicode(e)),
            'last_updated': datetime.datetime.now().isoformat()
        })
        raise


def resource_score(context, data):
    """
    Score resources on Sir Tim Berners-Lee\'s five stars of openness
    based on mime-type.

    returns a dict with keys:

        'openness_score': score (int)
        'openness_score_reason': the reason for the score (string)
        'openness_score_failure_count': the number of consecutive times that
                                        this resource has returned a score of 0
    """
    log = update.get_logger()

    score = 0
    score_reason = ''
    score_failure_count = 0

    # get openness score failure count for task status table if exists
    api_url = urlparse.urljoin(context['site_url'], 'api/action')
    response = requests.post(
        api_url + '/task_status_show',
        json.dumps({'entity_id': data['id'], 'task_type': 'qa',
                    'key': 'openness_score_failure_count'}),
        headers={'Authorization': context['apikey'],
                 'Content-Type': 'application/json'}
    )
    if json.loads(response.content)['success']:
        score_failure_count = int(json.loads(response.content)['result'].get('value', '0'))

    log = update.get_logger()
    # no score for resources that don't have an open license
    if not data.get('is_open'):
        score_reason = 'License not open'
    else:
        try:
            headers = json.loads(link_checker("{}", json.dumps(data)))
            ct = headers.get('content-type')

            # ignore charset if exists (just take everything before the ';')
            if ct and ';' in ct:
                ct = ct.split(';')[0]

            # also get format from resource and by guessing from file extension
            format = data.get('format', '').lower()
            file_type = mimetypes.guess_type(data['url'])[0]

            # file type takes priority for scoring
            score = -1
            if file_type:
                score = MIME_TYPE_SCORE.get(file_type, -1)
            if ct:
                score = max(score, MIME_TYPE_SCORE.get(ct, -1))
            if format:
                # we want to make sure the linked resource is what format
                # claims it is
                type_list = ALLOWED_MIME_TYPES.get(format, [])
                if len(type_list) == 0 or file_type in type_list:
                    score = max(score, MIME_TYPE_SCORE.get(format, -1))

            score_reason = OPENNESS_SCORE_REASON.get(score, "Unable to calculate openness score")

            # negative scores are only useful for getting the reason message,
            # set it back to 0 if it's still <0 at this point
            if score < 0:
                score = 0

            # check for mismatches between content-type, file_type and format
            # ideally they should all agree
            if not ct:
                # TODO: use the todo extension to flag this issue
                pass

        except LinkCheckerError, e:
            score_reason = str(e)
        except Exception, e:
            log.error('Unexpected error while calculating openness score %s: %s', e.__class__.__name__,  unicode(e))
            score_reason = "Unknown error: %s" % str(e)

    if score == 0:
        score_failure_count += 1
    else:
        score_failure_count = 0

    return {
        'openness_score': score,
        'openness_score_reason': score_reason,
        'openness_score_failure_count': score_failure_count,
    }
