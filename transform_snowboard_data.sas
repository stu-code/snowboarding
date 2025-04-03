
libname pq parquet '/workspaces/myfolder/data';
libname out '/workspaces/myfolder/data';

/* Fix timestamps in Heartrate data */
data hr;
    set pq.hr;

    /* Convert to MT */
    if(   '25JAN2024'd LE datepart(timestamp) LE '28JAN2024'd
       OR '13MAR2025'd LE datepart(timestamp) LE '15MAR2025'd)
        then timestamp = intnx('hour', timestamp, -7, 'S')
    ;

    /* Convert to ET */
    else if(   '23FEB2024'd LE datepart(timestamp) LE '24FEB2024'd 
            OR '09FEB2025'd LE datepart(timestamp) LE '10FEB2025'd)
        then timestamp = intnx('hour', timestamp, -5, 'S')
    ;
run;

/* Fuzzy merge GPS with heartrate data */
proc sql;
    create table _snowboarding_gps_hr(drop=dif) as
        select round(gps.timestamp) as timestamp format=datetime.2
             , datepart(gps.timestamp) as date format=date9.
             , gps.lat
             , gps.lon
             , round(gps.elevation)*3.28084 as elevation /* Convert from Meters to Feet */
             , round(gps.speed, .1) as speed
             , hr.bpm
             , hr.confidence as hr_sensor_confidence
             , abs(round(hr.timestamp) - round(gps.timestamp)) as dif
        from pq.gps
        LEFT JOIN
             hr
        ON   dhms(datepart(gps.timestamp), hour(gps.timestamp), minute(gps.timestamp), 0)
           = dhms(datepart(hr.timestamp), hour(hr.timestamp), minute(hr.timestamp), 0)
        where    timepart(hr.timestamp) BETWEEN '8:30't AND '16:00't      /* Retrieve some missing data */
              OR datepart(gps.timestamp) IN ('27JAN2024'd, '28JAN2024'd)  /* Retrieve some missing data */
        group by calculated timestamp
        having dif = min(dif)
    ;
quit;

/* Add in types: lift or run */
proc sql;
    create table snowboarding_gps_hr as
        select gps.*
               , meta.type
               , CASE(meta.type)
                     when('Lift') then meta.numberOfType
                     else .
                 END as lift_nbr
               , CASE(meta.type)
                     when('Run') then meta.numberOfType
                     else .
                 END as run_nbr
        from _snowboarding_gps_hr as gps
        LEFT JOIN
             pq.gps_meta as meta
        ON gps.timestamp BETWEEN meta.start AND meta.end
        order by gps.timestamp
    ;
quit;

/* De-dupe timestamps and output */
proc sort data=snowboarding_gps_hr 
          out=out.snowboarding_gps_hr(compress=yes) 
          nodupkey;
    by timestamp;
run;

/* Convert to sas7bdat */
data out.gps_meta;
    set pq.gps_meta;
run;