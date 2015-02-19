// [[Rcpp::depends(RcppEigen)]]

#include <Rcpp.h>
#include <RcppEigen.h>

using namespace Rcpp;

// inverts the singular values
// takes advantage of the fact that singular values are never negative
inline Eigen::ArrayXd Dsoft(const Eigen::ArrayXd& d, double penalty) {
  Eigen::ArrayXd di(d.size());
  for (int j = 0; j < d.size(); ++j) {
    double penalized = d[j] - penalty;
    if (penalized < 0) {
      di[j] = 0;
    } else {
      di[j] = penalized;
    }
  }
  return di;
}

//' Singular Value Thresholding on a numeric matrix
//' 
//' @param X numeric matrix
//' @param penalty scalar to penalize singular values 
//' @return a list with 2 components, the singular value thresholded matrix and its thresholded singular values
//' @export
// [[Rcpp::export]]
List RcppSVT(const Eigen::MatrixXd& X, double penalty) {
  const Eigen::JacobiSVD<Eigen::MatrixXd> UDV(X.jacobiSvd(Eigen::ComputeThinU|Eigen::ComputeThinV));
  const Eigen::ArrayXd Ds(Dsoft(UDV.singularValues(), penalty));
  const Eigen::MatrixXd S(UDV.matrixU() * Ds.matrix().asDiagonal() * UDV.matrixV().adjoint());
  
  return List::create(Named("Xhat") = S,
  Named("d.thresholded") = Ds);
}

// [[Rcpp::export]]
double RcppSoftThresholdScalar(double x, double penalty) {
  //sign(x) * pmax(abs(x) - penalty,0)
  double penalized = std::abs(x) - penalty;
  if (penalized < 0) return 0;
  if (x > 0) return penalized;
  return -penalized;
}

// [[Rcpp::export]]
Eigen::ArrayXd RcppSoftThresholdVector(const Eigen::ArrayXd& x, double penalty) {
  int n = x.size();
  Eigen::ArrayXd out(n);
  for (int i = 0; i < n; i++) {
    out[i] = RcppSoftThresholdScalar(x[i], penalty);
  }
  return out;
}

// [[Rcpp::export]]
Eigen::MatrixXd RcppSoftThresholdMatrix(const Eigen::MatrixXd& x, double penalty) {
  int m = x.rows();
  int n = x.cols();
  Eigen::MatrixXd out(m,n);
  for (int i = 0; i < m; i++) {
    for (int j = 0; j < n; j++) {
      out(i,j) = RcppSoftThresholdScalar(x(i,j), penalty);  
    }
  }
  return out;
}

double median_rcpp(NumericVector x) {
  NumericVector y = clone(x);
  int n, half;
  double y1, y2;
  n = y.size();
  half = n / 2;
  if(n % 2 == 1) {
    // median for odd length vector
    std::nth_element(y.begin(), y.begin()+half, y.end());
    return y[half];
  } else {
    // median for even length vector
    std::nth_element(y.begin(), y.begin()+half, y.end());
    y1 = y[half];
    std::nth_element(y.begin(), y.begin()+half-1, y.begin()+half);
    y2 = y[half-1];
    return (y1 + y2) / 2.0;
  }
}

double mad_rcpp(NumericVector x, double scale_factor = 1.4826) {
  return median_rcpp(abs(x - median_rcpp(x))) * scale_factor;
}

double getDynamicMu(Eigen::MatrixXd E) {
  int m = E.rows();
  int n = E.cols();
  
  NumericVector Evec = wrap(E.array());
  double E_sd = sd(Evec);
  double mu = 0;
  if (m > n) {
    mu = E_sd * sqrt(2 * m);
  } else {
    mu = E_sd * sqrt(2 * n);
  }
  if (mu < .01) return .01;
  return mu;
}

List getL(Eigen::MatrixXd X, Eigen::MatrixXd S, double mu, double L_penalty) {
  double L_penalty2 = L_penalty * mu;
  
  const Eigen::MatrixXd diff(X - S);
  
  List L = RcppSVT(diff, L_penalty2);
  double L_nuclearnorm = as<Eigen::ArrayXd>(L[1]).sum();
  return List::create(L[0], L_penalty2 * L_nuclearnorm);
}

List getS(Eigen::MatrixXd X, Eigen::MatrixXd L, double mu, double s_penalty) {
  double s_penalty2 = s_penalty * mu;
  
  const Eigen::MatrixXd diff(X - L);
  
  Eigen::MatrixXd S = RcppSoftThresholdMatrix(diff, s_penalty2);
  double S_l1norm = S.lpNorm<1>();
  return List::create(S, s_penalty2 * S_l1norm);
}

List getE(Eigen::MatrixXd X, Eigen::MatrixXd L, Eigen::MatrixXd S) {
  const Eigen::MatrixXd E(X - L - S);
  return List::create(E, E.squaredNorm());
}

double objective(double L, double S, double E) {
  return (.5*E) + L + S;
}

//' Robust Principal Component Pursuit
//' 
//' @param X numeric matrix
//' @param Lpenalty scalar to penalize singular values. 
//' A default of -1 is used as a sentinel value. Under this sentinel value
//' a smart thresholding algorithm sets the value of Lpenalty, so the user does not need to.
//' @param Spenalty scalar to penalize remainder matrix to find anomalous values.
//' A default of -1 is used as a sentinel value. Under this sentinel value
//' a smart thresholding algorithm setes the value of Spenalty, so the user does not need to.
//' @return a list with 4 matrices.  X is decomposed into L + S + E where L is low rank,
//' S is sparse and E is the remainder matrix of noise
//' @importFrom Rcpp evalCpp
//' @export
// [[Rcpp::export]]
List RcppRPCA(Eigen::MatrixXd X, double Lpenalty = -1, double Spenalty = -1, bool verbose = false) {
  int m = X.rows();
  int n = X.cols();
  
  if (Lpenalty == -1) {
    Lpenalty = 1;
  }
  if (Spenalty == -1) {
    if (m > n) {
      Spenalty = 1.4 / sqrt(m);
    } else {
      Spenalty = 1.4 / sqrt(n);
    }
  }
  
  int iter = 0;
  int maxIter = 1000;
  bool converged = false;
  double obj_prev = 0.5 * X.squaredNorm();
  double tol = 1e-8 * obj_prev;
  double diff = 2 * tol;
  double mu = m*n / (4*X.lpNorm<1>());
  
  double obj;
  Eigen::MatrixXd L_matrix = Eigen::MatrixXd::Zero(m,n);
  Eigen::MatrixXd S_matrix = Eigen::MatrixXd::Zero(m,n);
  Eigen::MatrixXd E_matrix = Eigen::MatrixXd::Zero(m,n);
  while (iter < maxIter & diff > tol) {
    List S = getS(X, L_matrix, mu, Spenalty);
    S_matrix = S[0];
    List L = getL(X, S_matrix, mu, Lpenalty);
    L_matrix = L[0];
    List E = getE(X, L_matrix, S_matrix);
    E_matrix = E[0];
    
    obj = objective(as<double>(L[1]), as<double>(S[1]), as<double>(E[1]));
    if (verbose) {
      Rcout << "Objective function: " << obj_prev << " on previous iteration " << iter << std::endl;
      Rcout << "Objective function: " << obj << " on iteration " << iter-1 << std::endl;
    }
    if (verbose) 
    diff = std::abs(obj_prev - obj);
    obj_prev = obj;
    mu = getDynamicMu(E_matrix);
    iter++;
    if (diff < tol) converged = true;
  }
  if (verbose) {
    if (!converged) Rcout << "Failed to converge within " << maxIter << "iterations" << std::endl;
    if (converged) Rcout << "Converged within " << iter << " iterations" << std::endl;
  }
  return List::create(Named("X") = X, Named("L") = L_matrix, Named("S") = S_matrix, Named("E") = E_matrix);
}
