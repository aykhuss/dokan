def plot_merge_and(**kwargs):
    gnuplot = r"""
set terminal pdfcairo enhanced color size 10cm,10cm dashed  # font "Iosevka,7" fontscale 0.65
set encoding utf8
set style fill transparent pattern border
set pointintervalbox 0.001
"""
    # > insert all the info into the template
    gnuplot += r"""
PATH  = '{path}'
OBS   = '{obs}'
MERGE = '{merge}'
PARTS = '{parts}'
""".format(**kwargs)
    gnuplot += r"""
merged = PATH.'/'.MERGE.'.'.OBS.'.dat'

#> parse the merged histogram file to extract some information on the data structure we're dealing with
eval system("awk 'BEGIN{bin_acc=0.;bin_num=-1;bin_lst=0.;xmin=0.;xmax=0.;}$1!~/^#/{bin_num++;if(bin_num==0){xmin=$1;}xmax=$3;if(bin_num>0){bin_acc+=($3-$1)/bin_lst;}bin_lst=$3-$1;}$1~/^#labels:/{printf(\"ncol = %d;\", gensub(/.+\\[([0-9]+)\\]$/, \"\\\\1\", \"g\", $(NF)));}$1~/^#nx:/{printf(\"nx = %d;\", $2);}END{printf(\"bin_fac = %e;\",bin_acc/bin_num);printf(\"xmin = %e;xmax = %e;\",xmin,xmax);}' ".merged)
#> only plots for distributions
if (nx != 3) quit;

label(i) = system(sprintf("awk '$1~/^#labels:/{print $(%d)}' ",i).merged)  # comment prefix causes one offset

#> (x-a)/(x+a) & only take into account error from `x`, values in ‰
ratio(part) = "< paste ".merged." ".part." | ".sprintf("awk 'BEGIN{nx=%d;ncol=%d;}$1!~/^#/&&$1==$(ncol+1)&&$(nx)==$(ncol+nx){for(i=1;i<=nx;i++){printf(\"%e \",$(i))}for(i=nx+1;i<ncol;i+=2){j=ncol+i;x=$(j);a=$(i);if(x==0.&&a==0.){val=0.;err=0.;}else{val=(x-a)/(x+a);err=$(j+1)*2.*a/(x+a)**2};printf(\" %e %e \",val*1e3,err*1e3);}printf(\"\\n\");}' 2> /dev/null", nx,ncol)
#> (x-a)/Δa & only take into account error from `x`
dev(part) = "< paste ".merged." ".part." | ".sprintf("awk 'BEGIN{nx=%d;ncol=%d;}$1!~/^#/&&$1==$(ncol+1)&&$(nx)==$(ncol+nx){for(i=1;i<=nx;i++){printf(\"%e \",$(i))}for(i=nx+1;i<ncol;i+=2){j=ncol+i;x=$(j);a=$(i);if(x==0.&&a==0.){val=0.;err=0.;}else{val=(x-a)/$(i+1);err=$(j+1)/$(i+1)};printf(\" %e %e \",val,err);}printf(\"\\n\");}' 2> /dev/null", nx,ncol)

set output MERGE.'.'.OBS.'.pdf'

if (bin_fac > 1.1) {
  set log x
  bin_loc(xlow,xupp,pos) = exp(log(xlow) + pos*(log(xupp)-log(xlow)))
} else {
  unset log x
  bin_loc(xlow,xupp,pos) = xlow + pos*(xupp-xlow)
}
set format y '%g'
set xrange [xmin:xmax]

set lmargin 10
set key top left horizontal
unset colorbox  # remove palette box

#> @TODO: injection point of custom commands?

do for [t = 1:(ncol-nx)/2] {
  col = nx+2*t-1;
  #set title label(col+1) noenhanced
  set multiplot layout 2,1 title label(col+1) noenhanced
  set ylabel '(X − ref) / (X + ref)  [‰]' noenhanced
  set format x ''
  unset xlabel
  plot \
    ratio(PATH.'/'.MERGE.'.'.OBS.'.dat')." | awk '$1!~/^#/{print $0;$1=$3;print $0;}'" u 1:(column(col)+column(col+1)):(column(col)-column(col+1)) w filledc lc rgbcolor 0 fs transparent solid 0.3 t MERGE noenhanced, \
    for [i=1:words(PARTS)] ratio(PATH.'/'.word(PARTS, i).'.'.OBS.'.dat') u (bin_loc($1,$3,0.25+i*0.5/(words(PARTS)+1.))):col:(column(col+1)) w err lc palette frac i/(words(PARTS)+1.) pt i ps 0.2 t word(PARTS, i) noenhanced
  set ylabel '(X − ref) / Δref' noenhanced
  set format x '%g'
  set xlabel OBS noenhanced
  plot \
    dev(PATH.'/'.MERGE.'.'.OBS.'.dat')." | awk '$1!~/^#/{print $0;$1=$3;print $0;}'" u 1:(column(col)+column(col+1)):(column(col)-column(col+1)) w filledc lc rgbcolor 0 fs transparent solid 0.3 notitle, \
    for [i=1:words(PARTS)] dev(PATH.'/'.word(PARTS, i).'.'.OBS.'.dat') u (bin_loc($1,$3,0.25+i*0.5/(words(PARTS)+1.))):col:(column(col+1)) w err lc palette frac i/(words(PARTS)+1.) pt i ps 0.2 notitle
  unset multiplot
}

unset output
"""
    return gnuplot


def plot_merge_plus(**kwargs):
    gnuplot = r"""
set terminal pdfcairo enhanced color size 10cm,15cm dashed  # font "Iosevka,7" fontscale 0.65
set encoding utf8
set style fill transparent pattern border
set pointintervalbox 0.001
"""
    # > insert all the info into the template
    gnuplot += r"""
PATH  = '{path}'
OBS   = '{obs}'
MERGE = '{merge}'
PARTS = '{parts}'
""".format(**kwargs)
    gnuplot += r"""
merged = PATH.'/'.MERGE.'.'.OBS.'.dat'

#> parse the merged histogram file to extract some information on the data structure we're dealing with
eval system("awk 'BEGIN{bin_acc=0.;bin_num=-1;bin_lst=0.;xmin=0.;xmax=0.;}$1!~/^#/{bin_num++;if(bin_num==0){xmin=$1;}xmax=$3;if(bin_num>0){bin_acc+=($3-$1)/bin_lst;}bin_lst=$3-$1;}$1~/^#labels:/{printf(\"ncol = %d;\", gensub(/.+\\[([0-9]+)\\]$/, \"\\\\1\", \"g\", $(NF)));}$1~/^#nx:/{printf(\"nx = %d;\", $2);}END{printf(\"bin_fac = %e;\",bin_acc/bin_num);printf(\"xmin = %e;xmax = %e;\",xmin,xmax);}' ".merged)
#> only plots for distributions
if (nx != 3) quit;

label(i) = system(sprintf("awk '$1~/^#labels:/{print $(%d)}' ",i).merged)  # comment prefix causes one offset

#> x/a & only take into account error from `x`, values in %
ratio(part) = "< paste ".merged." ".part." | ".sprintf("awk 'BEGIN{nx=%d;ncol=%d;}$1!~/^#/&&$1==$(ncol+1)&&$(nx)==$(ncol+nx){for(i=1;i<=nx;i++){printf(\"%e \",$(i))}for(i=nx+1;i<ncol;i+=2){j=ncol+i;x=$(j);a=$(i);if(x==0.&&a==0.){val=0.;err=0.;}else{val=x/a;err=$(j+1)/a};printf(\" %e %e \",val*1e2,err*1e2);}printf(\"\\n\");}' 2> /dev/null", nx,ncol)

set output MERGE.'.'.OBS.'.pdf'

if (bin_fac > 1.1) {
  set log x
  bin_loc(xlow,xupp,pos) = exp(log(xlow) + pos*(log(xupp)-log(xlow)))
} else {
  unset log x
  bin_loc(xlow,xupp,pos) = xlow + pos*(xupp-xlow)
}
set format y '%g'
set xrange [xmin:xmax]

set lmargin 10
set key top left horizontal
unset colorbox  # remove palette box

#> @TODO: injection point of custom commands

do for [t = 1:(ncol-nx)/2] {
  col = nx+2*t-1;
  #set title label(col+1) noenhanced
  set multiplot layout 3,1 title label(col+1) noenhanced
  set yrange [*:*]
  set tics
  set ylabel 'Δ' noenhanced
  set format x ''
  unset xlabel
  set log y
  plot \
    "< awk '$1!~/^#/{print $0;$1=$3;print $0;}' ".merged u 1:(column(col+1)) w l lc rgbcolor 0 t MERGE noenhanced, \
    for [i=1:words(PARTS)] PATH.'/'.word(PARTS, i).'.'.OBS.'.dat' u (bin_loc($1,$3,0.5)):(column(col+1)) w lp lc palette frac i/(words(PARTS)+1.) dt i pt i ps 0.2 notitle
  set ylabel 'X / ref  [%]' noenhanced
  set format x '%g'
  set xlabel OBS noenhanced
  unset log y
  plot \
    ratio(PATH.'/'.MERGE.'.'.OBS.'.dat')." | awk '$1!~/^#/{print $0;$1=$3;print $0;}'" u 1:(column(col)+column(col+1)):(column(col)-column(col+1)) w filledc lc rgbcolor 0 fs transparent solid 0.3 notitle, \
    for [i=1:words(PARTS)] ratio(PATH.'/'.word(PARTS, i).'.'.OBS.'.dat') u (bin_loc($1,$3,0.25+i*0.5/(words(PARTS)+1.))):col:(column(col+1)) w err lc palette frac i/(words(PARTS)+1.) pt i ps 0.2 notitle
  #> separate panel just for legends
  unset tics
  unset xlabel
  unset ylabel
  #set border 0
  set yrange [0:1]
  set key bottom center
    plot \
      2 w l lc rgbcolor 0 t MERGE noenhanced, \
      for [i=1:words(PARTS)] 2 w lp lc palette frac i/(words(PARTS)+1.) dt i pt i ps 0.2 t word(PARTS, i) noenhanced
  unset multiplot
}

unset output
"""
    return gnuplot
