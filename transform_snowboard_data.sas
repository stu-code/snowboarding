
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

/* Filter all rows to only be between the start and end of each run */
data gps_filtered;
    set pq.gps;

    if(_N_ = 1) then do;
        format type $10.;

        dcl hash meta(dataset: 'pq.gps_meta', ordered: 'yes');
            meta.defineKey('start', 'end');
            meta.defineData('start', 'end', 'type', 'numberOfType');
        meta.defineDone();

        dcl hiter iter('meta');

        call missing(start, end, type, numberOfType);
    end;

    rc = iter.first();

    do while(rc = 0);
        if(start <= timestamp <= end) then do;

            /* Identify lifts or runs */
            if(type = 'Run') then run_nbr = numberOfType;
                else lift_nbr = numberOfType;

            output;
            leave;
        end;

        rc = iter.next();
    end;

    drop start end type numberOfType rc;
run;

/* De-dupe timestamps and output */
proc sort data=snowboarding_gps_hr 
          out=out.snowboarding_gps_hr
          nodupkey;
    by timestamp;
run;

/* Convert to sas7bdat */
data out.gps_meta;
    set pq.gps_meta;
run;