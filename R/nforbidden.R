#' forbiden patterns
#'
#' This function calculates Number of "forbiden patterns" (cf. Amigo 2010)
#' Using package statcomp (https://www.rdocumentation.org/packages/statcomp/versions/0.0.1.1000/topics/global_complexity)
#'
#' @param time_serie input time series data
#' @param ndemb (OPTIONAL) If x is given, the embedding dimension (ndemb) is required.
#' @importFrom statcomp global_complexity
#' @return Number of "forbidden patterns" (cf. Amigo 2010)
measure.PE_nforbidden <- function(time_serie, ndemb) {
  # suppressMessages(library(statcomp))
  result = tryCatch({
    out=statcomp::global_complexity(x = time_serie, ndemb = ndemb)
    out=out[c(1,3)]
    names(out)=c("permutation_entropy","nforbidden")
    out
  }, error = function(e) {
    out=c(NA,NA)
    names(out)=c("permutation_entropy","nforbidden")
    out
  })
  return (result)
}



