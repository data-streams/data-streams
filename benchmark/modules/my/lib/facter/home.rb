Facter.add('home') do
  setcode do
    Facter::Util::Resolution.exec('echo $HOME')
  end
end
