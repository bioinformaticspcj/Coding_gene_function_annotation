#!/usr/bin/perl -w
#author:pcj
use Parallel::ForkManager;
use strict;
my($idmapping_file,$blast6_out_dir,$databas,$max_process);
if($#ARGV<0){
	&usage;
	exit;
	
}
$idmapping_file=shift;
$blast6_out_dir=shift;
#$databas=shift;
$max_process=shift;
$max_process||=4; 
sub usage{
	my $parameters=<<"USAGE";
	This script is to map NR or Swissprot or both ids
	to GO ids;
	idmapping file 	idmapping.tb.gz;
	annotation files dir contains blast out files in 6 formate or table files (first column: query second column:target);
	example:
	perl $0 idmapping.tb.gz blast_out > GO_anotation
	or:
	perl $0 idmapping.tb.gz blast_out 8(threads) > GO_anotation

USAGE
print $parameters;
exit;
}
my(%id_ref);
open IDMAP,"gzip -dc $idmapping_file|" or die "can not open $idmapping_file\n";
#open IDMAP,"$idmapping_file" or die "can not open $idmapping_file\n";
while(<IDMAP>){
	chomp;
	my @lines=split/\t/;
	$lines[7]=~s/\s+//;
	$lines[7]=~s/\;/ /;
	$id_ref{$lines[0]}=$lines[7];
	$id_ref{$lines[1]}=$lines[7];
	$id_ref{$lines[3]}=$lines[7];
}
close IDMAP;
opendir BLAST,"$blast6_out_dir" or die "can not open $blast6_out_dir\n";
my $pm = new Parallel::ForkManager($max_process);
LINE:
while(my $file=readdir BLAST){
	next if($file=~/^\.$/||$file=~/^\.\.$/);
	$pm->start and next LINE;
	open IN,"$blast6_out_dir/$file" or die "can not open $blast6_out_dir/$file\n";
	while(<IN>){
		chomp;
		next if($.==1);
		my @lines=split/\t/;
		my @swiss=split/\|/,$lines[1];
		#print"$swiss[1]\n";
		if(exists $id_ref{$lines[1]}|| exists $id_ref{$swiss[1]}){
			print "$lines[0]\t$id_ref{$swiss[1]}\n";
		}	
	}
	close IN;
	$pm->finish;
}
$pm->wait_all_children;
close  BLAST;

