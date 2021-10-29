#' Lempel-Ziv Complexity
#'
#' Based on https://github.com/benjaminfrot/LZ76/blob/master/C/LZ76.c
#'
#' @param time_serie A time serie, object of type ts.
#' @return The value of the complexity measure: Lempel-Ziv.
#'
measure.lempel_ziv <- function(time_serie) {
  result = tryCatch({
    n = length(time_serie)
    b = n/log2(n)
    c = measure.kolmogorov(time_serie)/b
    out=c
    out
  }, error = function(e) {
    out=NA
    out
  })
  names(result)="lempel_ziv"
  return (result)
}

#' Kolmogorov Complexity
#'
#' Based on http://www.mathworks.com/matlabcentral/fileexchange/6886-kolmogorov-complexity
#'
#' @param time_serie A time serie, object of type ts.
#' @return The value of the complexity measure: Kolmogorov.
#'
measure.kolmogorov <- function(time_serie) {
  result = tryCatch({
    # Encode the time series
    threshold = mean(time_serie)
    s <- vector()
    for (i in 1:length(time_serie)) {
      if (time_serie[i] < threshold) s[i] <- 0
      else s[i] <- 1
    }
    n = length(s)
    c = l = k = kmax = 1
    i = end = 0
    while (end == 0) {
      if (s[i+k] != s[l+k]) {
        if (k > kmax) {
          kmax = k
        }
        i = i + 1

        if (i == l) {
          c = c + 1
          l = l + kmax
          if (l + 1 > n) end = 1
          else {
            i = 0
            k = kmax = 1
          }
        } else {
          k = 1
        }
      } else {
        k = k + 1
        if (l + k > n) {
          c = c + 1
          end = 1
        }
      }
    }
    out=c
    out
  }, error = function(e) {
    out=NA
    out
  })

  names(result)="kolmogorov"
  return (result)
}

