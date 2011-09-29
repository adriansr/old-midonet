package com.midokura.midolman.layer3;

import java.io.Serializable;
import java.util.UUID;

public class Route implements Serializable {

    private static final long serialVersionUID = -5913569441176193396L;
    public static final int NO_GATEWAY = 0xffffffff;

    public enum NextHop {
        BLACKHOLE, REJECT, PORT;
    }

    public int srcNetworkAddr;
    public int srcNetworkLength;
    public int dstNetworkAddr;
    public int dstNetworkLength;
    public NextHop nextHop;
    public UUID nextHopPort;
    public int nextHopGateway;
    public int weight;
    public String attributes;
    public UUID routerId;

    public Route(int srcNetworkAddr, int srcNetworkLength, int dstNetworkAddr,
            int dstNetworkLength, NextHop nextHop, UUID nextHopPort,
            int nextHopGateway, int weight, String attributes, UUID routerId) {
        super();
        this.srcNetworkAddr = srcNetworkAddr;
        this.srcNetworkLength = srcNetworkLength;
        this.dstNetworkAddr = dstNetworkAddr;
        this.dstNetworkLength = dstNetworkLength;
        this.nextHop = nextHop;
        this.nextHopPort = nextHopPort;
        this.nextHopGateway = nextHopGateway;
        this.weight = weight;
        this.attributes = attributes;
        this.routerId = routerId;
    }

    // Default constructor for the Jackson deserialization.
    public Route() { super(); }

    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;
        if (other == this)
            return true;
        if (!(other instanceof Route))
            return false;
        Route rt = (Route) other;
        if (null == nextHop || null == rt.nextHop) {
            if (nextHop != rt.nextHop)
                return false;
        } else if (!nextHop.equals(rt.nextHop))
            return false;
        if (null == nextHopPort || null == rt.nextHopPort) {
            if (nextHopPort != rt.nextHopPort)
                return false;
        } else if (!nextHopPort.equals(rt.nextHopPort))
            return false;
        if (null == attributes || null == rt.attributes) {
            if (attributes != rt.attributes)
                return false;
        } else if (!attributes.equals(rt.attributes))
            return false;
        if (null == routerId || null == rt.routerId) {
            if (routerId != rt.routerId)
                return false;
        } else if (!routerId.equals(rt.routerId))
            return false;
        
        return dstNetworkAddr == rt.dstNetworkAddr
                && dstNetworkLength == rt.dstNetworkLength
                && srcNetworkAddr == rt.srcNetworkAddr
                && srcNetworkLength == rt.srcNetworkLength
                && nextHopGateway == rt.nextHopGateway && weight == rt.weight;
    }

    @Override
    public int hashCode() {
        int hash = 1;
        hash = 13 * hash + srcNetworkAddr;
        hash = 17 * hash + srcNetworkLength;
        hash = 31 * hash + dstNetworkAddr;
        hash = 23 * hash + dstNetworkLength;
        hash = 37 * hash + nextHopGateway;
        hash = 11 * hash + weight;
        
        if (null != routerId)
            hash = 47 * hash + routerId.hashCode();
        if (null != nextHop)
            hash = 29 * hash + nextHop.hashCode();
        if (null != nextHopPort)
            hash = 43 * hash + nextHopPort.hashCode();
        if (null != attributes)
            hash = 5 * hash + attributes.hashCode();
        return hash;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(srcNetworkAddr).append(",");
        sb.append(srcNetworkLength).append(",");
        sb.append(dstNetworkAddr).append(",");
        sb.append(dstNetworkLength).append(",");
        if (null != nextHop)
            sb.append(nextHop.toString());
        sb.append(",");
        if (null != nextHopPort)
            sb.append(nextHopPort.toString());
        sb.append(",");
        sb.append(nextHopGateway).append(",");
        sb.append(weight).append(",");
        if (null != attributes)
            sb.append(attributes);
        if (null != routerId)
            sb.append(routerId);
        return sb.toString();
    }

    public static Route fromString(String str) {
        String[] parts = str.split(",");
        Route rt = new Route(Integer.parseInt(parts[0]),
                Byte.parseByte(parts[1]), Integer.parseInt(parts[2]),
                Byte.parseByte(parts[3]), parts[4].isEmpty() ? null
                        : NextHop.valueOf(parts[4]), parts[5].isEmpty() ? null
                        : UUID.fromString(parts[5]),
                Integer.parseInt(parts[6]), Integer.parseInt(parts[7]),
                parts.length > 8? parts[8] : null, null);
        return rt;
    }

}
