plan my::localhost(
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
    ::ssh::client::config::user { system::env('USER'):
      target => "${facts['home']}/.ssh/flamestream-benchmarks.config",
      ensure => present,
      options => $worker_host_private_ip.keys().map |String $host| {
        {
          "Host $host" => {
            'User' => 'ubuntu',
            'ProxyJump' => 'flamestream-benchmarks-manager',
          },
        }
      }.reduce({
        'Host flamestream-benchmarks-manager' => {
          'User' => 'ubuntu',
        },
      }) |Hash $options, Hash $worker| { $options + $worker },
    }
    exec { "/usr/bin/ssh -oStrictHostKeyChecking=no flamestream-benchmarks-manager exit": }
  }
}
