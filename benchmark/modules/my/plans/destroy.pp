plan my::destroy(
  String $manager_public_ip,
  String $manager_private_ip,
  Array[String] $worker_private_ips,
) {
  $worker_host_private_ip = $worker_private_ips.map |Integer $index, String $ip| {
    { "flamestream-benchmarks-worker-$index" => $ip }
  }.reduce({}) |Hash[String, String] $all, Hash[String, String] $worker| { $all + $worker }
  $host_private_ip = $worker_host_private_ip + { 'flamestream-benchmarks-manager' => $manager_private_ip }

  'localhost'.apply_prep
  apply('localhost') {
    $ssh_known_hosts_file = "${facts['home']}/.ssh/known_hosts"
    sshkey { 'flamestream-benchmarks-manager':
      ensure => absent,
      host_aliases => $manager_public_ip,
      target => $ssh_known_hosts_file,
    }
    $worker_host_private_ip.each |String $host, String $private_ip| {
      sshkey { $host:
        ensure => absent,
        host_aliases => $private_ip,
        target => $ssh_known_hosts_file,
      }
    }
  }
}
