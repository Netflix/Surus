-- Register JARs
REGISTER 's3n://path-to-surus/surus.jar';
REGISTER 's3n://path-to-datafu/datafu.jar';

DEFINE rpca_outliers_daily (inputBag, nWeeks, dateColumnName, metricColumnNames)
returns rpca_outliers_daily
/*
 * This macro will take a grouped bag and use RPCA for daily outlier detection.  It expects
 * that you have already aggregated the data at a daily grain, performed a "group by" on the keys
 * of interest, and an ordered inner_date_bag.  As an example you could imagine a bag with the
 * following structure:
 *
 *     grunt> describe input_data_bag;
 *     input_data_bag: {
 *         group: (
 *              .... (some high cardinality group) ....
 *         ),
 *         inner_data_bag: {
 *             (
 *                 utc_dateint: chararray,
 *                 metric_1: long,
 *                 metric_2: long,
 *                 ... (other columns?) ....
 *             )
 *         }
 *     }
 *
 * The function expects that the "inner_data_bag" be complete (no missing date values)
 * and ordered (e.g. 20141101,20141102,...) which is required by the RPCA function.  With
 * this data structure you can simply use the MACRO rpca_outliers_daily to calculate the outliers.
 *
 *     grunt> output_data_bag = rpca_outliers_daily(input_data_bag, nWeeks, 'utc_dateint', 'metric_1,metric_2')
 *     grunt> describe output_data_bag;
 *     output_data_bag: {
 *         flatten(group),
 *         metric: chararray, -- [metric_1, metric_2]
 *         com_netflix_dse_outlier_evalrpca: {
 *             (
 *                 utc_dateint: chararray,
 *                 value: long,
 *                 x_transform: double,
 *                 rsvd_l: double,
 *                 rsvd_s: double,
 *                 rsvd_e: double
 *             )
 *         }
 *     }
 *
 *  Example of the script being used can be found here:
 *      -- 
 *
 */
{
    -- RPCA Constructor
    DEFINE RPCA org.surus.pig.RAD('value','7','$nWeeks');
    
    -- Required for simultaneously process multiple metrics 
    DEFINE TransposeTupleToBag datafu.pig.util.TransposeTupleToBag();
    DEFINE BagGroupMacro       datafu.pig.bags.BagGroup();
    DEFINE Coalesce            datafu.pig.util.Coalesce('lazy');
    
    -- Performs the data transpose required to process multiple metrics simultaneously
    inputBag_exploded = foreach $inputBag {
        -- explode the bag to process multiple metrics
        inputBag_temp = foreach $1 generate $dateColumnName, flatten(TransposeTupleToBag($metricColumnNames));
        
        -- in-memory group on the metric
        GENERATE $0 as input_group
               , BagGroupMacro(inputBag_temp, inputBag_temp.key) as backfilled_new;
    }
    
    -- flattens in-memory group ... The original grain of inputBag was (group), and now has the grain (group,key)
    inputBag_by_metric = foreach inputBag_exploded generate input_group, flatten(backfilled_new);
    
    -- coalesce values and process outliers
    $rpca_outliers_daily = foreach inputBag_by_metric {
        -- coalese empty values
        inputBag_clean = foreach inputBag_temp generate $dateColumnName, Coalesce($2,0L) as value;
        
        -- finally, we can process the outliers
        generate flatten(input_group), group as metric, RPCA(inputBag_clean);
    }

};
