plan my::bench(
  String $manager_public_ip,
  String $manager_private_ip,
  Array[String] $worker_private_ips,
) {
  'flamestream-benchmarks-manager'.apply_prep
  apply('flamestream-benchmarks-manager') {
    vcsrepo { "${facts['home']}/FlameStream":
      ensure   => present,
      provider => git,
      source   => 'https://github.com/flame-stream/FlameStream',
      revision => 'feature/acker-bench',
    }
    file { "${facts['home']}/FlameStream/benchmark/ansible/remote.yml":
      content => inline_template(@(ERB))
<%= { "all" => { "children" => {
  "bench" => { "hosts" => { "flamestream-benchmarks-manager" => {} } },
  "manager" => { "hosts" => { "flamestream-benchmarks-manager" => {} } },
  "workers" => { "hosts" => @worker_private_ips.each_index.map do |index|
    ["flamestream-benchmarks-worker-#{index}", {}]
  end.to_h },
} } }.to_yaml %>
ERB
    }
  }
}
