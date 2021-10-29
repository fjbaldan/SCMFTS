#' Calculate some interesting features of the time series based on the tsfeatures package
#'
#' Using package tsfeatures (https://www.rdocumentation.org/packages/tsfeatures/versions/1.0.1)
#' Methods for extracting various features from time series data. The features provided are those
#' from Hyndman, Wang and Laptev (2013) <doi:10.1109/ICDMW.2015.104>, Kang, Hyndman and
#' Smith-Miles (2017) <doi:10.1016/j.ijforecast.2016.09.004> and from Fulcher,
#' Little and Jones (2013) <doi:10.1098/rsif.2013.0048>.
#'
#' @param ts imput time series.
#' @importFrom tsfeatures tsfeatures
tsfeaturesH<-function(ts,fast){
  methods=c(
     "acf_features",
     "max_kl_shift",
     "outlierinclude_mdrmd",
     "max_level_shift",
     "ac_9",
     "crossing_points",
     "max_var_shift",
     "spreadrandomlocal_meantaul",
     "flat_spots",
     "pacf_features",
    "trev_num",
    "holt_parameters",
    "walker_propcross",
    "hurst",
    "histogram_mode",
    "hw_parameters",
    "lumpiness",
    "motiftwo_entro3"
            )
  if(fast==FALSE){
    methods=c(
      "acf_features",
      "max_kl_shift",
      "outlierinclude_mdrmd",
      "max_level_shift",
      "ac_9",
      "crossing_points",
      "max_var_shift",
      "nonlinearity", #
      "embed2_incircle",#
      "spreadrandomlocal_meantaul",
      "flat_spots",
      "pacf_features",
      "firstmin_ac", #
      "std1st_der", #
      "stability", #
      "firstzero_ac", #
      "trev_num",
      "holt_parameters",
      "stl_features", #
      "walker_propcross",
      "hurst",
      "unitroot_kpss", #
      "histogram_mode",
      "hw_parameters",
      "unitroot_pp", #
      "localsimple_taures", #
      "lumpiness",
      "motiftwo_entro3"
    )
  }

  out=sapply(methods, function(method){
    tryCatch({
      out=NA
      if(method=="embed2_incircle"){
        out=tsfeatures::tsfeatures(tslist=ts,scale=F, features=method, parallel = FALSE,boundary = 1)
      }else{
        out=tsfeatures::tsfeatures(tslist=ts,scale=F, features=method, parallel = FALSE)

      }
      out
    }, error = function(e) {
      ts=ts(1:100,frequency = 10)
      struct = tsfeatures::tsfeatures(ts,scale=F, features=method , parallel = FALSE)
      fe=names(struct)
      out=rep(NA, length(fe))
      names(out)=fe
      out
    })
  })
  out=unlist(out)
  out
}
