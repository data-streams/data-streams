plan my::hosts(
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
    $all_hosts.each |String $host, String $ip| { host { $host: ip => $ip } }
  }

  $worker_hosts.keys().apply_prep
  apply($worker_hosts.keys()) {
    $all_hosts.each |String $host, String $ip| { host { $host: ip => $ip } }
  }
}
