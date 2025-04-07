/* The purpose of this program is to transform Slopes GPS, GPS Metadata, and 
   FitBit heart rate data into a format for analysis. This program does the following:
   
   1. Converts timestamps in heart rate data to MT or ET, depending on date
   2. Merges GPS metadata to GPS data to identify lifts and runs, and remove unneeded timestamps
   3. Fuzzy-merges GPS data with heart rate data such that the heart rate timestamp is with the closest GPS timestamp 

   This program should be run after extract_snowboard_data.ipynb

   Expected Datasets:
       - pq.hr       | FitBit heart rate data in Parquet format
       - pq.gps      | GPS data in Parquet format
       - pq.gpa_meta | GPS metadata in Parquet format
*/

/* Location of input and output datasets */
libname pq parquet '/workspaces/myfolder/data';             /* Input */
libname out '/workspaces/myfolder/snowboarding/data/final'; /* Output */
libname outpq parquet '/workspaces/myfolder/snowboarding/data/final'; /* Output (parquet) */

/* Fix timestamps in Heart Rate data */
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

/* Determine run or lift number for each timestamp and filter out
   timestamps that aren't a part of GPS metadata intervals */
data gps_filtered;
    set pq.gps;
    retain start end type numberOfType rc;

    if(_N_ = 1) then do;
        length type $4.;

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

/* Fuzzy merge GPS with heartrate data by associating the heart rate with the
   nearest GPS timestamp */
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
data outpq.snowboarding_gps_hr;
    set out.snowboarding_gps_hr;
run;

/* Copy pq GPS metadata to final output location */
data outpq.gps_meta;
    set pq.gps_meta;
run;

/* Convert GPS metadata to SAS dataset */
data out.gps_meta(compress=yes);
    set pq.gps_meta;
run;