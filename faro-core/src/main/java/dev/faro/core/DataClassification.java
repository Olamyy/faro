package dev.faro.core;

/**
 * Data classification for a captured feature value.
 *
 * <p>Controls whether {@code entity_id} and {@code feature_value} are emitted in ENTITY mode.
 * {@code PERSONAL} and {@code SENSITIVE} features degrade to AGGREGATE capture — neither
 * extractor is called and no entity identifier appears in the emitted event.
 *
 * <table>
 *   <tr><th>Classification</th><th>entity_id</th><th>feature_value</th><th>capture_mode</th></tr>
 *   <tr><td>NON_PERSONAL</td><td>emitted</td><td>emitted</td><td>ENTITY</td></tr>
 *   <tr><td>PSEUDONYMOUS</td><td>emitted</td><td>emitted</td><td>ENTITY</td></tr>
 *   <tr><td>PERSONAL</td><td>suppressed</td><td>suppressed</td><td>AGGREGATE</td></tr>
 *   <tr><td>SENSITIVE</td><td>suppressed</td><td>suppressed</td><td>AGGREGATE</td></tr>
 * </table>
 */
public enum DataClassification {
    NON_PERSONAL,
    PSEUDONYMOUS,
    PERSONAL,
    SENSITIVE
}
