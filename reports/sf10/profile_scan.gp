set terminal svg size 1200,850 dynamic noenhanced font 'DejaVu Sans,12' background rgb 'white'
set output 'profile_scan.svg'
set datafile separator "\t"
set multiplot layout 1,2 rowsfirst title 'SF10 unsorted ADBC scan: where time and CPU go' font 'DejaVu Sans,17'
set style data histograms
set style histogram clustered gap 1
set style fill solid 0.9 border -1
set boxwidth 0.8
set grid ytics lc rgb '#dddddd'
set border lc rgb '#555555'
set yrange [0:*]
set key top left box opaque
set title 'Elapsed time'
set ylabel 'Seconds (processor activity overlaps query time)'
plot 'profile_scan.tsv' using 2:xtic(1) title 'DuckDB query' lc rgb '#2369a8', \
     '' using 3 title 'ClickHouse Arrow output active' lc rgb '#b45320'
set title 'CPU time'
set ylabel 'Core-seconds'
plot 'profile_scan.tsv' using 4:xtic(1) title 'ClickHouse server' lc rgb '#39815a', \
     '' using 5 title 'DuckDB client process' lc rgb '#7953a8'
unset multiplot
