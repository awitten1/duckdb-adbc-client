set terminal svg size 1200,850 dynamic noenhanced font 'DejaVu Sans,12' background rgb 'white'
set output 'curl_formats.svg'
set multiplot layout 1,3 rowsfirst title 'SF10 orders: complete sorted curl transfers' font 'DejaVu Sans,17'
set datafile separator "\t"
set style fill solid 0.85 border -1
set boxwidth 0.55 relative
set grid ytics lc rgb '#dddddd'
set border lc rgb '#555555'
set xrange [-0.6:1.6]
set yrange [0:*]
set key off
set xtics rotate by -25

set title 'End-to-end curl wall time'
set ylabel 'Seconds (lower is better)'
plot 'curl_formats.tsv' using 0:2:xtic(1) with boxes lc rgb '#2369a8', \
     '' using 0:2:3:4 with yerrorbars lc rgb '#202020' lw 1.5

set title 'Output-format processor elapsed'
set ylabel 'Seconds (includes downstream waits)'
plot 'curl_formats.tsv' using 0:5:xtic(1) with boxes lc rgb '#b45320'

set title 'HTTP bytes sent'
set ylabel 'Decimal GB (lower is smaller)'
plot 'curl_formats.tsv' using 0:6:xtic(1) with boxes lc rgb '#39815a'
unset multiplot
