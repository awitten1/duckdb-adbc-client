set terminal svg size 1200,850 dynamic noenhanced font 'DejaVu Sans,12' background rgb 'white'
set output 'adbc_compression.svg'
set multiplot layout 1,2 rowsfirst title 'SF10 orders: ADBC HTTP response compression' font 'DejaVu Sans,17'
set datafile separator "\t"
set style data histograms
set style histogram clustered gap 1
set style fill solid 0.9 border -1
set boxwidth 0.85
set grid ytics lc rgb '#dddddd'
set border lc rgb '#555555'
set yrange [0:4.1]
set xtics rotate by -25
set ylabel 'EXPLAIN ANALYZE query time (seconds; lower is better)'
set key top left box opaque

set title 'Sort key: o_orderdate'
plot 'compression_date.tsv' using 2:xtic(1) title 'LZ4 response compression' lc rgb '#2369a8', \
     '' using 3 title 'Compression off' lc rgb '#39815a'

set title 'Sort key: o_totalprice'
unset key
plot 'compression_price.tsv' using 2:xtic(1) title 'LZ4 response compression' lc rgb '#2369a8', \
     '' using 3 title 'Compression off' lc rgb '#39815a'
unset multiplot
