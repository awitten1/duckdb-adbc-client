set terminal svg size 1200,850 dynamic noenhanced font 'DejaVu Sans,12' background rgb 'white'
set output 'perf_schema_off_times.svg'
set datafile separator "\t"
set title 'SF10 with schema-only connector and HTTP compression off' font 'DejaVu Sans,17'
set style data histograms
set style histogram clustered gap 1
set style fill solid 0.9 border -1
set boxwidth 0.8
set grid ytics lc rgb '#dddddd'
set border lc rgb '#555555'
set yrange [0:3.3]
set ylabel 'EXPLAIN ANALYZE query time (seconds; lower is better)'
set key top left box opaque
plot 'perf_schema_off_times.tsv' using 2:xtic(1) title 'Date, 9 columns' lc rgb '#2369a8', \
     '' using 3 title 'Price, 9 columns' lc rgb '#b45320', \
     '' using 4 title 'Price, 2 columns' lc rgb '#39815a'
