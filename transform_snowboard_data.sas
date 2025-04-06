
libname pq parquet '/workspaces/myfolder/data';
libname out '/workspaces/myfolder/data';

/* Fix timestamps in Heartrate data */
data hr;
    set pq.hr;

    date = datepart(timestamp);

    /* Convert to MT */
    if(   '25JAN2024'd <= date <= '28JAN2024'd
       OR '13MAR2025'd <= date <= '15MAR2025'd)
    then timestamp = intnx('hour', timestamp, -7, 'S');

    /* Convert to ET */
    else if(   '23FEB2024'd <= date <= '24FEB2024'd 
            OR '09FEB2025'd <= date <= '10FEB2025'd)
    then timestamp = intnx('hour', timestamp, -5, 'S');
    
    drop date;
run;

/* Filter all rows to only be between the start and end of each run */
data gps_filtered;
    set pq.gps;
    retain start end type numberOfType rc;

    if(_N_ = 1) then do;
        format type $10.;

        dcl hash meta(dataset: 'pq.gps_meta', ordered: 'yes');
            meta.defineKey('start', 'end');
            meta.defineData('start', 'end', 'type', 'numberOfType');
        meta.defineDone();

        dcl hiter iter('meta');

        call missing(start, end, type, numberOfType);

        /* Get the first value from the hash table */
        rc = iter.first();
    end;

    /* If we'ved to a new run, get the next timestamp */
    if(timestamp > end) then rc = iter.next();

    /* As long as there's a value from the hash table and the
       GPS timestamp is between the start/end points of the
       metadata timestamp, then get the run/lift number and output */
    if(rc = 0 and start <= timestamp <= end) then do;
        if(type = 'Run') then run_nbr = numberOfType;
            else lift_nbr = numberOfType;
        output;
    end;

    drop start end type numberOfType rc;
run;

/* Fuzzy merge GPS with heartrate data */
proc sql;
    create table snowboarding_gps_hr(drop=dif) as
        select round(gps.timestamp) as timestamp format=datetime.2
             , gps.lat
             , gps.lon
             , gps.lift_nbr
             , gps.run_nbr
             , gps.elevation*3.28084 as elevation
             , gps.speed
             , hr.bpm
             , hr.confidence as hr_sensor_confidence
             , abs(round(hr.timestamp) - round(gps.timestamp)) as dif
        from gps_filtered as gps
        left join
             hr
        on   dhms(datepart(gps.timestamp), hour(gps.timestamp), minute(gps.timestamp), 0)
           = dhms(datepart(hr.timestamp), hour(hr.timestamp), minute(hr.timestamp), 0)
        group by calculated timestamp
        having dif = min(dif)
    ;
quit;

/* De-dupe timestamps and output */
proc sort data=snowboarding_gps_hr 
          out=out.snowboarding_gps_hr(compress=yes) 
          nodupkey;
    by timestamp;
run;

/* Save main dataset as parquet */
data pq.snowboarding_gps_hr;
    set out.snowboarding_gps_hr;
run;

/* Convert GPA metadata to sas7bdat */
data out.gps_meta(compress=yes);
    set pq.gps_meta;
run;