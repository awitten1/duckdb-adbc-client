set terminal svg size 1200,850 dynamic noenhanced font 'DejaVu Sans,12' background rgb 'white'
set output 'alpha_sweep.svg'
set multiplot layout 2,2 rowsfirst title 'SF10 orders: alpha sweep by sort key'
set datafile separator "\t"
set grid ytics lc rgb '#dddddd'
set border lc rgb '#555555'
set xrange [-0.05:1.05]
set yrange [0:*]
set xtics ('0' 0, '.25' .25, '.5' .5, '.75' .75, '1' 1)
set xlabel 'Alpha: key-range fraction sorted remotely'
set ylabel 'Query time (seconds; lower is better)'
set key bottom right opaque box spacing 1.0 font 'DejaVu Sans,10'
set style line 1 lc rgb '#2369a8' lw 2.5 pt 7 ps 0.9
set style line 2 lc rgb '#b45320' lw 2 dt 2
set style line 3 lc rgb '#338269' lw 2 dt 3
set style line 4 lc rgb '#666666' lw 2 dt 4
set title 'o_orderkey (physical order)'
plot 'alpha_orderkey.tsv' using 1:2:3:4 with yerrorbars lc rgb '#2369a8' pt 7 notitle, \
     'alpha_orderkey.tsv' using 1:2 with linespoints ls 1 title 'Hybrid', \
     'alpha_orderkey.tsv' using 1:5 with lines ls 2 title 'DuckDB', \
     'alpha_orderkey.tsv' using 1:6 with lines ls 3 title 'Remote', \
     'alpha_orderkey.tsv' using 1:7 with lines ls 4 title 'Unsorted read'
set title 'o_orderdate'
plot 'alpha_orderdate.tsv' using 1:2:3:4 with yerrorbars lc rgb '#2369a8' pt 7 notitle, \
     'alpha_orderdate.tsv' using 1:2 with linespoints ls 1 title 'Hybrid', \
     'alpha_orderdate.tsv' using 1:5 with lines ls 2 title 'DuckDB', \
     'alpha_orderdate.tsv' using 1:6 with lines ls 3 title 'Remote', \
     'alpha_orderdate.tsv' using 1:7 with lines ls 4 title 'Unsorted read'
set title 'o_totalprice'
plot 'alpha_totalprice.tsv' using 1:2:3:4 with yerrorbars lc rgb '#2369a8' pt 7 notitle, \
     'alpha_totalprice.tsv' using 1:2 with linespoints ls 1 title 'Hybrid', \
     'alpha_totalprice.tsv' using 1:5 with lines ls 2 title 'DuckDB', \
     'alpha_totalprice.tsv' using 1:6 with lines ls 3 title 'Remote', \
     'alpha_totalprice.tsv' using 1:7 with lines ls 4 title 'Unsorted read'
set title 'o_clerk'
plot 'alpha_clerk.tsv' using 1:2:3:4 with yerrorbars lc rgb '#2369a8' pt 7 notitle, \
     'alpha_clerk.tsv' using 1:2 with linespoints ls 1 title 'Hybrid', \
     'alpha_clerk.tsv' using 1:5 with lines ls 2 title 'DuckDB', \
     'alpha_clerk.tsv' using 1:6 with lines ls 3 title 'Remote', \
     'alpha_clerk.tsv' using 1:7 with lines ls 4 title 'Unsorted read'
unset multiplot
