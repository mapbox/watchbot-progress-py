from watchbot_progress import Part, create_job

parts = [
    {'source': 'a.tif'},
    {'source': 'b.tif'},
    {'source': 'c.tif'}]


def annotate_parts(parts):
    return [
        dict(**part, partid=partid, jobid=jobid)
        for partid, part in enumerate(parts)]


if __name__ == "__main__":
    # ------------- Subject: start-job
    jobid = create_job(parts)

    # ------------- Subject: map
    for msg in annotate_parts(parts):
        with Part(msg):
            partid = msg['partid']
            print(f'PROCCESS {partid}')
            print('PROCCESS complete')

    print("complete")
