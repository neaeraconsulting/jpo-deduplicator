/**
 * This package contains topologies which can optionally be
 * activated for filtering out messages based on configured criteria.
 * These filters go beyond deduplicating redundant messages and will include
 * "lossy" operations that actually remove data, for example:
 * <ul>
 *     <li>Filter BSMs based on fixed bits in the Temp ID</li>
 *     <li>Geofencing Operations</li>
 * </ul>
 *
 */
package us.dot.its.jpo.deduplicator.filters;