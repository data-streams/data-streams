plan my::install(
  String $manager_public_ip,
  String $manager_private_ip,
  Array[String] $worker_private_ips,
) {
  'flamestream-benchmarks-manager'.apply_prep
  apply('flamestream-benchmarks-manager') {
    include apt
    apt::ppa { 'ppa:deadsnakes/ppa': }
    package { 'git': }
    package { 'python3.7': }
    package { 'maven': }
    package { 'python3': }
    package { 'python3-distutils': }
    class { 'python::pip::bootstrap': version => 'pip3' }
    python::pip { 'ansible': }
  }
}
