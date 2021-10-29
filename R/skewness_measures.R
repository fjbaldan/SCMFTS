#' Estimation of Skewness
#'
#' Using https://cran.r-project.org/web/packages/e1071/e1071.pdf
#'
#' @param time_serie A time serie, object of type ts.
#' @importFrom e1071 skewness
#' @return The value of the Skewness measure
measure.skewness <- function(time_serie) {
  result = tryCatch({
    e1071::skewness(time_serie)
  }, error = function(e) {
    NA
  })
  names(result)="skewness"
  return (result)
}
