plan my::install(
  String $manager_public_ip,
  String $manager_private_ip,
  Array[String] $worker_private_ips,
) {
  $worker_hosts = $worker_private_ips.map |Integer $index, String $ip| {
    { "flamestream-benchmarks-worker-$index" => $ip }
  }.reduce({}) |Hash[String, String] $all, Hash[String, String] $worker| { $all + $worker }
  $all_hosts = $worker_hosts + { 'flamestream-benchmarks-manager' => $manager_private_ip }

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
  $all_hosts.keys().apply_prep
  apply($all_hosts.keys()) {
    include apt
    package { 'openjdk-8-jdk': }
    package { 'rsync': }
    package { 'vim': }
    package { 'less': }
    package { 'htop': }
    package { 'procps': }
    package { 'unzip': }
    package { 'sysstat': }
    package { 'make': }
    package { 'gcc': }
    alternatives { java: path => '/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java' }
  }
}
