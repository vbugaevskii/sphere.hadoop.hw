package ru.mail.sphere.hadoop.hbase;

import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;

public class TestRobotsFilter {
    @Test
    public void testSimpleCase() throws RobotsFilter.BadFormatException {
        RobotsFilter filter = new RobotsFilter("Disallow: /users");

        assertTrue(filter.isAllowed("/company/about.html"));

        assertFalse(filter.isAllowed("/users/jan"));
        assertFalse(filter.isAllowed("/users/"));
        assertFalse(filter.isAllowed("/users"));

        assertTrue("should be allowed since in the middle", filter.isAllowed("/another/prefix/users/about.html"));
        assertTrue("should be allowed since at the end", filter.isAllowed("/another/prefix/users"));
    }

    @Test
    public void testEmptyCase() {
        RobotsFilter filter = new RobotsFilter();

        assertTrue(filter.isAllowed("/company/about.html"));
        assertTrue(filter.isAllowed("/company/second.html"));
        assertTrue(filter.isAllowed("any_url"));
    }

    @Test
    public void testEmptyStringCase() throws RobotsFilter.BadFormatException {
        // that's different from testEmptyCase() since we
        // explicitly pass empty robots_txt rules
        RobotsFilter filter = new RobotsFilter("");

        assertTrue(filter.isAllowed("/company/about.html"));
        assertTrue(filter.isAllowed("/company/second.html"));
        assertTrue(filter.isAllowed("any_url"));
    }

    @Test
    public void testRuleEscaping() throws RobotsFilter.BadFormatException {
        // we have to escape special characters in rules (like ".")
        RobotsFilter filter = new RobotsFilter("Disallow: *.php$");

        assertFalse(filter.isAllowed("file.php"));
        assertTrue("sphp != .php", filter.isAllowed("file.sphp"));
    }

    @Test(expected = RobotsFilter.BadFormatException.class)
    public void testBadFormatException() throws RobotsFilter.BadFormatException {
        RobotsFilter filter = new RobotsFilter("Allowed: /users");
    }

    @Test
    public void testAllCases() throws RobotsFilter.BadFormatException {
        String rules = "Disallow: /users\n" +
                "Disallow: *.php$\n" +
                "Disallow: */cgi-bin/\n" +
                "Disallow: /very/secret.page.html$\n";

        RobotsFilter filter = new RobotsFilter(rules);

        assertFalse(filter.isAllowed("/users/jan"));
        assertTrue("should be allowed since in the middle", filter.isAllowed("/subdir2/users/about.html"));

        assertFalse(filter.isAllowed("/info.php"));
        assertTrue("we disallowed only the endler", filter.isAllowed("/info.php?user=123"));
        assertTrue(filter.isAllowed("/info.pl"));

        assertFalse(filter.isAllowed("/forum/cgi-bin/send?user=123"));
        assertFalse(filter.isAllowed("/forum/cgi-bin/"));
        assertFalse(filter.isAllowed("/cgi-bin/"));
        assertTrue(filter.isAllowed("/scgi-bin/"));


        assertFalse(filter.isAllowed("/very/secret.page.html"));
        assertTrue("we disallowed only the whole match", filter.isAllowed("/the/very/secret.page.html"));
        assertTrue("we disallowed only the whole match", filter.isAllowed("/very/secret.page.html?blah"));
        assertTrue("we disallowed only the whole match", filter.isAllowed("/the/very/secret.page.html?blah"));
    }
}