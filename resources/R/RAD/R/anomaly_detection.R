#' Time Series Anomaly Detection
#' 
#' Fast C++ implementation of time series anomaly detection using Robust Principal Component Pursuit
#' @param X a vector representing a time series, or a data frame where columns are time series.
#' The length of this vector should be divisible by frequency.
#' If X is a vector it will be cast to a matrix of dimension frequency by length(X)/frequency
#' @param frequency the frequency of the seasonality of X
#' @param dates optional vector of dates to be used as a time index in the output
#' @param autodiff boolean. If true, use the Augmented Dickey Fuller Test to determine
#' if differencing is needed to make X stationary
#' @param forcediff boolean. If true, always compute differences
#' @param scale boolean. If true normalize the time series to zero mean and unit variance
#' @param L.penalty a scalar for the amount of thresholding in determining the low rank approximation for X.
#' The default values are chosen to correspond to the smart thresholding values described in Candes'
#' Stable Principal Component Pursuit
#' @param s.penalty a scalar for the amount of thresholding in determining the separation between noise and sparse outliers
#' The default values are chosen to correspond to the smart thresholding values described in Zhou's
#' Stable Principal Component Pursuit
#' @param verbose boolean. If true print status updates while running optimization program
#' @useDynLib RAD
#' @importFrom tseries adf.test
#' @details Robust Principal Component Pursuit is a matrix decomposition algorithm that seeks
#' to separate a matrix X into the sum of three parts X = L + S + E. L is a low rank matrix representing
#' a smooth X, S is a sparse matrix containing corrupted data, and E is noise. To convert a time series
#' into the matrix X we take advantage of seasonality so that each column represents one full period, for
#' example for weekly seasonality each row is a day of week and one column is one full week.
#' 
#' While computing the low rank matrix L we take an SVD of X and soft threshold the singular values.
#' This approach allows us to dampen all anomalies across the board simultaneously making the method
#' robust to multiple anomalies. Most techniques such as time series regression and moving averages
#' are not robust when there are two or more anomalies present.
#' 
#' Empirical tests show that identifying anomalies is easier if X is stationary.
#' The Augmented Dickey Fuller Test is used to test for stationarity - if X is not stationary
#' then the time series is differenced before calling RPCP. While this test is abstracted away
#' from the user differencing can be forced by setting the forcediff parameter.
#' 
#' The thresholding values can be tuned for different applications, however we strongly
#' recommend using the defaults which were proposed by Zhou.
#' For more details on the choice of L.penalty and s.penalty
#' please refer to Zhou's 2010 paper on Stable Principal Component Pursuit.
#' 
#' The implementation of RPCP is done in C++ for high performance through RCpp.
#' This function simply preprocesses the time series and calls RcppRPCP. 
#' @return 
#' \itemize{
#'   \item X_transform. The transformation applied to the time series,
#'   can be the identity or could be differencing
#'   \item L_transform. The low rank component in the transformed space
#'   \item S_transform. The sparse outliers in the transformed space
#'   \item E_transform. The noise in the transformed space
#'   \item X_original. The original time series
#'   \item time. The time index
#'   \item name. The name of the time series if X was a named data frame
#' }
#' @references
#' The following are recommended educational material:
#' \itemize{
#'   \item Candes' paper on RPCP \url{http://statweb.stanford.edu/~candes/papers/RobustPCA.pdf}
#'   \item Zhou's follow up paper on Stable PCP \url{http://arxiv.org/abs/1001.2363}
#'   \item Metamarkets Tech Blog on anomalies in time \url{https://metamarkets.com/2012/algorithmic-trendspotting-the-meaning-of-interesting/}
#' }
#' @export
#' @examples
#' frequency = 7
#' numPeriods = 10
#' ts.sinusoidal = sin((2 * pi / frequency ) * 1:(numPeriods * frequency))
#' ts = ts.sinusoidal
#' ts = sin((2 * pi / frequency ) * 1:(numPeriods * frequency))
#' ts[58:60] = 100
#' ggplot_AnomalyDetection.rpca(AnomalyDetection.rpca(ts)) + ggplot2::theme_grey(base_size = 25)
AnomalyDetection.rpca = function(X, frequency=7, dates=NULL,
                                 autodiff = T,
                                 forcediff = F,
                                 scale = T,
                                 L.penalty = 1,
                                 s.penalty=1.4 / sqrt(max(frequency, ifelse(is.data.frame(X), nrow(X), length(X)) / frequency)),
                                 verbose=F) {
  if (is.vector(X) & !is.data.frame(X)) X = data.frame(y=X)
  time = if (is.null(dates)) 1:nrow(X) else dates
  
  #look through columns which are separate time series
  #transform each column vector into a matrix with nrow = observations per period
  #the number of columns will be equal to the number of periods
  rpca.ts = apply(X, 2, function(j) {
    j.init = j[1]
    useddiff = F
    if (forcediff) {
      useddiff = T
      j = c(0, diff(j))
    }
    else if (autodiff) {
      adf = suppressWarnings(tseries::adf.test(j))
      if (adf$p.value > .05) {useddiff = T; j = c(0, diff(j))}
    } 
    
    if (scale) {
      j.global.mean = mean(j)
      j.global.sd = sd(j)
      j.matrix.standard.global = matrix((j - j.global.mean) / j.global.sd, nrow = frequency)
      j.matrix = j.matrix.standard.global  
    } else {
      j.global.mean = 0
      j.global.sd = 1
      j.matrix = matrix(j, nrow = frequency)
    }
    
    list(rpca = RcppRPCA(j.matrix, 
                         Lpenalty = L.penalty, Spenalty = s.penalty, 
                         verbose=verbose),
         mean = j.global.mean,
         sd = j.global.sd,
         diff = useddiff,
         j.init = j.init
    )
  })
  rpca.ts.stacked = lapply(rpca.ts, function(i) {
    if (i$diff) {
      X.orig = c(i$j.init + cumsum((as.vector(i$rpca$X)) * i$sd + i$mean))
      X.transform = (as.vector(i$rpca$X)) * i$sd + i$mean
      L.transform = (as.vector(i$rpca$L)) * i$sd + i$mean
      S.transform = (as.vector(i$rpca$S)) * i$sd
      E.transform = (as.vector(i$rpca$E)) * i$sd
      
      L.orig = cumsum(L.transform) + i$j.init
      X.rough = X.orig - L.orig
      
      #S.orig = cumsum(S.transform)
      #E.orig = X.orig - L.orig - S.orig
      
      ###       
      #
      #X.rough.rpca = RcppRPCA(matrix(X.rough, nrow(i$rpca$X), ncol(i$rpca$X)),
      #                        Lpenalty = 10,
      #                        Spenalty = 2 / sqrt(10))
      #S.orig = as.numeric(X.rough.rpca$S)
      #E.orig = X.orig - L.orig - S.orig
      
      ###
      S.orig = softThreshold(X.rough, 3 * (1/sqrt(2)) * sd(E.transform))
      E.orig = X.orig - (L.orig) - S.orig
      
      data.frame(X.transform = X.transform,
                 L.transform = L.transform,
                 S.transform = S.transform,
                 E.transform = E.transform,
                 X.orig = X.orig,
                 time = time)[-1,]
    }
    else {
      data.frame(X.transform = (as.vector(i$rpca$X)) * i$sd + i$mean,
                 L.transform = (as.vector(i$rpca$L)) * i$sd + i$mean,
                 S.transform = (as.vector(i$rpca$S)) * i$sd,
                 E.transform = (as.vector(i$rpca$E)) * i$sd,
                 X.orig = (as.vector(i$rpca$X)) * i$sd + i$mean,
                 time = time)
    }
  })
  names = unlist((mapply(function(df, name) { rep(name, nrow(df)) }, rpca.ts.stacked, names(rpca.ts))))
  #build a report containing anomaly data for all the columns found in X
  rpca.ts.stacked = cbind(do.call('rbind', rpca.ts.stacked), name = as.vector(names))
  names(rpca.ts.stacked) = c("X_transform", "L_transform", "S_transform", "E_transform",
                             "X_original",
                             "time", "name")
  
  return (rpca.ts.stacked)
}

#' ggplot for AnomalyDetection
#' 
#' ggplot function which shows the low rank signal in blue, the random noise in green,
#' and any outliers in red. If a transformation was applied, these signals will be plotted
#' in the transformed space, along with the original time series
#' @param anomalyDetection output from AnomalyDetection.rpca
#' @import ggplot2
#' @export
#' @examples
#' frequency = 7
#' numPeriods = 10
#' ts.sinusoidal = sin((2 * pi / frequency ) * 1:(numPeriods * frequency))
#' ts = ts.sinusoidal
#' ts = sin((2 * pi / frequency ) * 1:(numPeriods * frequency))
#' ts[58:60] = 100
#' ggplot_AnomalyDetection.rpca(AnomalyDetection.rpca(ts)) + ggplot2::theme_grey(base_size = 25)
ggplot_AnomalyDetection.rpca = function(anomalyDetection) {
  ggplot2::ggplot(anomalyDetection, ggplot2::aes(time, X_original)) +
    ggplot2::geom_line(size = 1) +
    ggplot2::geom_line(ggplot2::aes(y = X_transform), size = 1, color = "black", linetype = 'dashed') +
    ggplot2::geom_line(ggplot2::aes(y = L_transform), size = .5, color = "blue") +
    ggplot2::geom_line(ggplot2::aes(y = E_transform), size = .5, color = "green") +
    ggplot2::geom_point(data = subset(anomalyDetection, abs(S_transform) > 0), color = "red",
               ggplot2::aes(size = abs(S_transform))) +
    ggplot2::scale_size_continuous(range=c(4,6)) +
    ggplot2::facet_wrap(~name, scale = "free")    
}

softThreshold = function(x, penalty) {
  sign(x) * pmax(abs(x) - penalty,0)
}
