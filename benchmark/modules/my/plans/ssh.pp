plan my::ssh(
  String $manager_public_ip,
  String $manager_private_ip,
  Array[String] $worker_private_ips,
) {
  $host_private_ip = $worker_private_ips.map |Integer $index, String $ip| {
    { "flamestream-benchmarks-worker-$index" => $ip }
  }.reduce({
    'flamestream-benchmarks-manager' => $manager_private_ip,
  }) |Hash[String, String] $all, Hash[String, String] $worker| { $all + $worker }

  $host_private_ip.keys().apply_prep
  $host_pub = zip($host_private_ip.keys(), get_targets($host_private_ip.keys())).map |Array $host| {
    { $host[0] => facts($host[1])['/etc/ssh/ssh_host_ecdsa_key.pub'].split(' ') }
  }.reduce({}) |Hash[String, Array[String]] $all, Hash[String, Array[String]] $host| { $all + $host }

  'localhost'.apply_prep
  apply('localhost') {
    $host_pub.each |String $host, Array[String] $pub| {
      sshkey { $host:
        type => $pub[0],
        key => $pub[1],
        host_aliases =>
          if ($host == 'flamestream-benchmarks-manager') { $manager_public_ip } else { $host_private_ip[$host] },
        target => "${facts['home']}/.ssh/known_hosts",
      }
    }
  }
  apply('flamestream-benchmarks-manager') {
    exec { "/usr/bin/test -e /home/ubuntu/.ssh/id_rsa.pub || ssh-keygen -N '' -f /home/ubuntu/.ssh/id_rsa": }
    $host_pub.each |String $host, Array[String] $pub| {
      sshkey { $host:
        type => $pub[0],
        key => $pub[1],
        host_aliases => $host_private_ip[$host],
        target => "${facts['home']}/.ssh/known_hosts",
      }
    }
  }

  'flamestream-benchmarks-manager'.apply_prep
  $id_rsa_pub_values = split(facts(get_targets('flamestream-benchmarks-manager')[0])['~/.ssh/id_rsa.pub'], ' ')
  apply($host_private_ip.keys()) {
    ssh_authorized_key { $id_rsa_pub_values[2]:
      user   => 'ubuntu',
      type   => $id_rsa_pub_values[0],
      key    => $id_rsa_pub_values[1],
    }
  }
}
