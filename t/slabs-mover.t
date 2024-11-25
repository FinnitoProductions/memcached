#!/usr/bin/env perl

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;
use Data::Dumper qw/Dumper/;

# Enable manual slab reassign, cap at 6 slabs
# Items above 16kb are chunked
my $server = new_memcached('-o slab_reassign,slab_automove=0,slab_chunk_max=16 -m 12');
my $sock = $server->sock;

{
    subtest 'syntax' => \&test_syntax;
    subtest 'fill and move pages' => \&test_fill;
    subtest 'chunked items' => \&test_chunked;
    # test locked items
    # test reflocked items and busy looping
    # test reflocked chunked items (ensure busy_deletes)
}

sub test_chunked {
    # Patterned value so we can tell if chunks get corrupted
    my $value;
    my $size = 30000;
    {
        my @chars = ("C".."Z");
        for (1 .. $size) {
            $value .= $chars[rand @chars];
        }
    }

    my $stats = mem_stats($sock);

    my $count = 1;
    while (1) {
        print $sock "set cfoo$count 0 0 $size\r\n$value\r\n";
        is(scalar <$sock>, "STORED\r\n", "stored chunked item");
        my $s_after = mem_stats($sock);
        last if ($s_after->{evictions} > $stats->{evictions});
        $count++;
    }

    # hold stats to compare evictions later
    $stats = mem_stats($sock);
    my $s = mem_stats($sock, 'slabs');
    my $sid = 0;
    # Find the highest ID to source from.
    for my $k (keys %$s) {
        next unless $k =~ m/^(\d+):/;
        $sid = $1 if $1 > $sid;
    }

    cmp_ok($s->{"$sid:total_pages"}, '>', 5, "most pages in high class");
    cmp_ok($s->{"$sid:used_chunks"}, '>', 500, "many chunks in use");

    # move one page while full to force evictions
    print $sock "slabs reassign $sid 0\r\n";
    my $stats_a;
    for (1 .. 20) {
        $stats_a = mem_stats($sock);
        last if ($stats_a->{slabs_moved} > $stats->{slabs_moved});
        sleep 0.25;
    }
    cmp_ok($stats_a->{evictions}, '>', $stats->{evictions}, "page move caused some evictions: " . ($stats_a->{evictions} - $stats->{evictions}));

    # delete at least a page worth so we can test rescuing data
    # NOTE: check for free_chunks first
}

# Fill test, no chunked items.
sub test_fill {
    my $size = 15000;
    my $bigdata = 'x' x $size;
    my $stats;
    for (1 .. 10000) {
        print $sock "set bfoo$_ 0 0 $size\r\n", $bigdata, "\r\n";
        is(scalar <$sock>, "STORED\r\n", "stored big key");
        $stats = mem_stats($sock);
        last if ($stats->{evictions} != 0);
    }

    # fill a smaller slab too
    $size = 2000;
    my $smalldata = 'y' x $size;
    for (1 .. 10000) {
        print $sock "set sfoo$_ 0 0 $size\r\n", $smalldata, "\r\n";
        is(scalar <$sock>, "STORED\r\n", "stored small key");
        my $nstats = mem_stats($sock);
        last if ($stats->{evictions} < $nstats->{evictions});
    }

    my $slabs_before = mem_stats($sock, "slabs");
    #print STDERR Dumper($slabs_before), "\n\n";
    # Find our two slab classes.
    # low, high
    my @classes = sort map { /(\d+):/ } grep {/total_pages/} keys %$slabs_before;
    is(scalar @classes, 2, "right number of active classes");
    my $cls_small = $classes[0];
    my $cls_big = $classes[1];

    $stats = mem_stats($sock);
    is($stats->{slabs_moved}, 0, "no slabs moved before testing");
    is($stats->{evictions}, 2, "only two total evictions before testing");
    print $sock "slabs reassign $cls_big $cls_small\r\n";
    is(scalar <$sock>, "OK\r\n", "slab rebalancer started: $cls_big -> $cls_small");

    for (1 .. 20) {
        $stats = mem_stats($sock);
        last if ($stats->{slabs_moved} != 0);
        sleep 0.25;
    }

    isnt($stats->{slabs_moved}, 0, "slab moved within time limit");
    my $slabs_after = mem_stats($sock, "slabs");
    isnt($slabs_before->{"$cls_small:total_pages"}, $slabs_after->{"$cls_small:total_pages"},
        "slab $cls_small pagecount changed");
    isnt($slabs_before->{"$cls_big:total_pages"}, $slabs_after->{"$cls_big:total_pages"},
        "slab $cls_big pagecount changed");
    cmp_ok($stats->{slab_reassign_busy_nomem}, '>', 1, 'busy looped due to lack of memory');
    cmp_ok($stats->{evictions}, '>', 10, 'ran normal evictions to move page');
    # inline reclaim and other stats might be nonzero: evicted, then tried to
    # allocate memory and it was from the page we intended to move.
    # not intentionally causing that here.

    # Move another page
    my $stats_after;
    print $sock "slabs reassign $cls_big $cls_small\r\n";
    is(scalar <$sock>, "OK\r\n", "slab rebalancer started: $cls_big -> $cls_small");

    for (1 .. 20) {
        $stats_after = mem_stats($sock);
        last if ($stats_after->{slabs_moved} != $stats->{slabs_moved});
        sleep 0.25;
    }

    cmp_ok($stats_after->{slabs_moved}, '>', $stats->{slabs_moved}, 'moved another page');

    # move all possible pages back to global
    empty_class(mem_stats($sock), $cls_big);
    empty_class(mem_stats($sock), $cls_small);
    $stats = mem_stats($sock);
    is($stats->{slab_global_page_pool}, 10, "pages back in global pool");

    #print STDERR Dumper(map { $_ => $stats->{$_} } sort keys %$stats), "\n";
}

sub empty_class {
    my $stats = shift;
    my $cls = shift;
    my $stats_after = $stats;
    while (1) {
        $stats = $stats_after;
        print $sock "slabs reassign $cls 0\r\n";
        my $res = <$sock>;
        if ($res =~ m/NOSPARE/) {
            pass("NOSPARE received while moving pages");
            last;
        }
        is($res, "OK\r\n", "slab rebalancer started: $cls -> 0");
        for (1 .. 20) {
            $stats_after = mem_stats($sock);
            last if ($stats_after->{slabs_moved} > $stats->{slabs_moved});
            sleep 0.25;
        }
    }
}

sub test_syntax {
    my $stats = mem_stats($sock, ' settings');
    is($stats->{slab_reassign}, "yes");

    print $sock "slabs reassign invalid1 invalid2\r\n";
    is(scalar <$sock>, "CLIENT_ERROR bad command line format\r\n");

    print $sock "slabs reassign 5\r\n";
    is(scalar <$sock>, "ERROR\r\n");

    print $sock "slabs reassign 1 1\r\n";
    is(scalar <$sock>, "SAME src and dst class are identical\r\n");

    print $sock "slabs reassign -5 70\r\n";
    is(scalar <$sock>, "BADCLASS invalid src or dst class id\r\n");

    print $sock "slabs reassign 2 1\r\n";
    is(scalar <$sock>, "NOSPARE source class has no spare pages\r\n");
}

done_testing();
