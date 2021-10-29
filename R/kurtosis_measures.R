#' Estimation of Kurtosis
#'
#' Using https://cran.r-project.org/web/packages/e1071/e1071.pdf
#'
#' @param time_serie A time serie, object of type ts.
#' @importFrom e1071 kurtosis
#' @return The value of the Kurtosis measure
measure.kurtosis <- function(time_serie) {
  result = tryCatch({
    e1071::kurtosis(time_serie)
  }, error = function(e) {
    NA
  })
  names(result)="kurtosis"
  return (result)
}
