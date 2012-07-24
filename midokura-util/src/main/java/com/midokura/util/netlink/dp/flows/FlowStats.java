/*
* Copyright 2012 Midokura Europe SARL
*/
package com.midokura.util.netlink.dp.flows;

import com.midokura.util.netlink.NetlinkMessage;
import com.midokura.util.netlink.messages.BaseBuilder;
import com.midokura.util.netlink.messages.BuilderAware;

public class FlowStats implements BuilderAware {

    /** Number of matched packets. */
    /*__u64*/ long n_packets;

    /** Number of matched bytes. */
    /*__u64*/ long n_bytes;

    @Override
    public void serialize(BaseBuilder builder) {
        builder.addValue(n_packets);
        builder.addValue(n_bytes);
    }

    @Override
    public boolean deserialize(NetlinkMessage message) {
        try {
            n_packets = message.getLong();
            n_bytes = message.getLong();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public long getNoPackets() {
        return n_packets;
    }

    public FlowStats setNoPackets(long nPackets) {
        this.n_packets = nPackets;
        return this;
    }

    public long getNoBytes() {
        return n_packets;
    }

    public FlowStats setNoBytes(long noBytes) {
        this.n_bytes = noBytes;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FlowStats flowStats = (FlowStats) o;

        if (n_bytes != flowStats.n_bytes) return false;
        if (n_packets != flowStats.n_packets) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (n_packets ^ (n_packets >>> 32));
        result = 31 * result + (int) (n_bytes ^ (n_bytes >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "FlowStats{" +
            "n_packets=" + n_packets +
            ", n_bytes=" + n_bytes +
            '}';
    }
}
