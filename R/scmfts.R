#' Calculates the complexity measures of a dataframe
#'
#' Gets the complexity measures of a dataframe composed by
#' time series. Each row contains the values of a time series.
#'
#'@param tsList List of time series.
#'@param n_cores Number of cores for parallel procesing (integer).
#'@param na Presence of NAs in the output dataframe (logical). It is recommended to use True in this parameter.
#'@param timeout Maximum calculation time allowed per method.
#'@return A dataframe containing the 41 time series characteristics for each time series processed.
#'@importFrom R.utils withTimeout
#'@importFrom parallel mclapply detectCores
#'@importFrom dplyr bind_rows
#'@import stats
#'@export
scmfts <- function(tsList=NULL, n_cores=(parallel::detectCores()-1), na=T, scale=T, timeout=999999, fast=F) {
  if(is.null(tsList)==TRUE || !is.list(tsList)){
    return(0)
  }

  run <- function(time_serie) {
    result=NULL
    if(!is.ts(time_serie)){
      time_serie=ts(time_serie,frequency=1)
    }
    if(scale==T){
      if(length(unique(time_serie))>1){
        time_serie=ts(as.numeric(base::scale(time_serie)),frequency = attr(time_serie,which = "tsp")[3])
      }
    }
    # # Time series characteristics
    aux= R.utils::withTimeout({measure.shannonEntropy(time_serie,"CS")}, timeout = timeout, onTimeout = "silent")
    result <- c(result, if(is.null(aux)) measure.shannonEntropy(NA,"CS") else aux )

    aux= R.utils::withTimeout({measure.shannonEntropy(time_serie,"SG")}, timeout = timeout, onTimeout = "silent")
    result <- c(result, if(is.null(aux)) measure.shannonEntropy(NA,"SG") else aux )

    aux= R.utils::withTimeout({measure.spectral_entropy(time_serie)}, timeout = timeout, onTimeout = "silent")
    result <- c(result, if(is.null(aux)) measure.spectral_entropy(NA) else aux )

    aux= R.utils::withTimeout({measure.PE_nforbidden(time_serie,6)}, timeout = timeout, onTimeout = "silent")
    result <- c(result, if(is.null(aux)) measure.PE_nforbidden(NA) else aux )

    aux= R.utils::withTimeout({measure.kurtosis(time_serie)}, timeout = timeout, onTimeout = "silent")
    result <- c(result, if(is.null(aux)) measure.kurtosis(NA) else aux )

    aux= R.utils::withTimeout({measure.skewness(time_serie)}, timeout = timeout, onTimeout = "silent")
    result <- c(result, if(is.null(aux)) measure.skewness(NA) else aux )

    aux= R.utils::withTimeout({tsfeaturesH(time_serie,fast=fast)}, timeout = timeout, onTimeout = "silent")
    result <- c(result, if(is.null(aux)) sapply(tsfeaturesH(as.vector(0)),function(x){NA}) else aux )

    result
  }

  if(.Platform$OS.type=="windows"){
    results <- parallel::mclapply(tsList, run, mc.cores=1)
  }else{
    results <- parallel::mclapply(tsList, run, mc.cores=n_cores)
  }

  # Optional elimination of NA
  if(na==F){
    a=which(lapply(results,function(x){
      sum(is.na(x))
    })>=1)
    if(length(a)>=1){
      results=results[-a]
    }
  }
  n_timeseries=length(results)

  if(!is.null(results)) {
    experiment_result=dplyr::bind_rows(results)
    rownames(experiment_result) <- NULL
  }

  return (experiment_result)
}

