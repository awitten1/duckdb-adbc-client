set terminal svg size 1200,850 dynamic noenhanced font 'DejaVu Sans,12' background rgb 'white'
set output 'projection_width.svg'
set datafile separator "\t"
set title 'SF10 orders: effect of full-row versus two-column projection' font 'DejaVu Sans,17'
set style data histograms
set style histogram clustered gap 1
set style fill solid 0.9 border -1
set boxwidth 0.85
set grid ytics lc rgb '#dddddd'
set border lc rgb '#555555'
set ylabel 'EXPLAIN ANALYZE query time (seconds; lower is better)'
set xlabel 'Sort key: o_totalprice; partition key: o_orderkey'
set xtics rotate by -20
set key top left box opaque
plot 'projection_width.tsv' using 2:xtic(1) title 'All 9 columns' lc rgb '#2369a8', \
     '' using 3 title '2 columns' lc rgb '#e38d2c'
