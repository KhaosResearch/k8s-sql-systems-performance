DRILL_QUERIES=("SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-1011\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-1011\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-1041\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-1041\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-1081\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-1081\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-1121\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-1121\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-1241\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-1241\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-1481\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-1481\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-1661\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-1661\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-partitioned-1011\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-partitioned-1011\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-partitioned-1021\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-partitioned-1021\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-partitioned-1041\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-partitioned-1041\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-partitioned-1081\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-partitioned-1081\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-partitioned-1121\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-partitioned-1121\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-partitioned-1241\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-partitioned-1241\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-partitioned-1481\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;"
"SELECT  cast(floor(((propagated_longitude/180) + 1)/2 * 4194304/8) * 8                                        AS bigint) AS px         ,cast(floor((ln(tan((90 + propagated_latitude) * 3.141592 / 360))/ 3.141592 + 1) / 2 * 4194304/ 8) * 8 AS bigint) AS py         ,LEAST((SUM(sum_cqi)/SUM(count_cqi)),15.0)                                                                        AS value         ,COUNT(*)                                                                                                         AS nsamples FROM dfs.\`ericsson-partitioned-1481\` WHERE count_cqi <> 0  GROUP BY  px           ,py HAVING LEAST((SUM(sum_cqi)/SUM(count_cqi)), 15.0) IS NOT NULL;")