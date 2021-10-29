#' Spectral Entropy
#'
#' Calculation of the value of the spectral entropy.
#' Based on: https://rdrr.io/cran/ForeCA/man/spectral_entropy.html
#'
#' @param y input time series data
#' @import ForeCA
#' @return spectral entropy
measure.spectral_entropy <- function(y) {
  result = tryCatch({
    entropy=ForeCA::spectral_entropy(y)
  }, error = function(e) {
    out=NA
    out
  })
  names(result)="spectral_entropy"
  return(result)
}

