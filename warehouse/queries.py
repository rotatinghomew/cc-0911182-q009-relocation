from .storage import records


def overview(connection):
    return {'resources':records(connection,'SELECT r.resource_id,r.name,count(d.row_id) AS row_count FROM resources r LEFT JOIN resource_rows d USING(resource_id) GROUP BY r.resource_id,r.name ORDER BY r.resource_id'), 'snapshots':records(connection,'SELECT * FROM snapshots ORDER BY snapshot_id')}
