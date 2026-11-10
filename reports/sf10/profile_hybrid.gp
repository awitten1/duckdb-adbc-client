set terminal svg size 1200,850 dynamic noenhanced font 'DejaVu Sans,12' background rgb 'white'
set output 'profile_hybrid.svg'
set datafile separator "\t"
set title 'SF10 alpha 0.5: output-thread merge time, compression off' font 'DejaVu Sans,17'
set style data histograms
set style histogram rowstacked
set style fill solid 0.9 border -1
set boxwidth 0.65
set grid ytics lc rgb '#dddddd'
set border lc rgb '#555555'
set yrange [0:*]
set ylabel 'Seconds on output thread (lower is better)'
set key top left box opaque
plot 'profile_hybrid.tsv' using 2:xtic(1) title 'Merge loop: compare and append' lc rgb '#b45320', \
     '' using 3 title 'Local chunk fetch' lc rgb '#2369a8', \
     '' using 4 title 'Remote chunk fetch' lc rgb '#39815a', \
     '' using 5 title 'Projection' lc rgb '#7953a8'
