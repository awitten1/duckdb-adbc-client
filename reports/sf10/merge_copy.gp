set terminal svg size 1200,850 dynamic noenhanced font 'DejaVu Sans,12' background rgb 'white'
set output 'merge_copy.svg'
set multiplot layout 1,2 rowsfirst title 'SF10 orders at alpha 0.5: merge behavior'
set datafile separator "\t"
set style fill solid 0.85 border -1
set boxwidth 0.62 relative
set grid ytics lc rgb '#dddddd'
set border lc rgb '#555555'
set xtics rotate by -30
set key off
set yrange [0:*]
set title 'Rows copied into output chunks'
set ylabel 'Rows copied (millions of 15 million)'
plot 'merge_alpha05.tsv' using 2:xtic(1) with boxes lc rgb '#2369a8'
set title 'Elapsed merge timer'
set ylabel 'Seconds (includes waits for remote input)'
plot 'merge_alpha05.tsv' using 3:xtic(1) with boxes lc rgb '#b45320'
unset multiplot
