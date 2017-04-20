package ru.mail.sphere.hadoop.hbase;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

import java.util.regex.Pattern;

public class RobotsFilter {
    public class BadFormatException extends Exception {

    }

    private static Pattern SPECIAL_REGEX_CHARS = Pattern.compile("([(){}.?|^+])");
    private static Pattern SPECIAL_REGEX_STARS = Pattern.compile("\\*");

    private List<String> rules = new LinkedList<>();
    private static final int numberSkip = "Disallow: ".length();

    public RobotsFilter() {}

    public RobotsFilter(String robots) throws BadFormatException {
        addRules(robots);
    }

    public void addRules(String robots) throws BadFormatException {
        for (String rule : robots.split("\n")) {
            if (!rule.isEmpty()) {
                if (rule.matches("Disallow: .*")) {
                    rule = rule.substring(numberSkip);
                    rules.add(ruleAsRegExp(rule));
                } else {
                    throw new BadFormatException();
                }
            }
        }
    }

    public boolean isAllowed(String path) {
        path = preprocessPath(path);

        for (String rule : rules) {
            if (path.matches(rule)) {
                return false;
            }
        }
        return true;
    }

    private String preprocessPath(String path) {
        try {
            URL url = new URL(path);

            StringBuilder result = new StringBuilder();

            if (url.getPath() != null) {
                result.append(url.getPath());
            }

            if (url.getQuery() != null) {
                result.append("?");
                result.append(url.getQuery());
            }

            if (url.getRef() != null) {
                result.append("#");
                result.append(url.getRef());
            }

            return result.length() > 0 ? result.toString() : path;
        } catch (MalformedURLException err) {
            return path;
        }
    }

    private String ruleAsRegExp(String rule) {
        if (!rule.endsWith("$")) {
            rule = rule + "*$";
        }

        rule = SPECIAL_REGEX_CHARS.matcher(rule).replaceAll("\\\\$0");
        rule = SPECIAL_REGEX_STARS.matcher(rule).replaceAll(".*");
        return rule;
    }

    public static void main(String[] args) throws Exception {
        RobotsFilter filter = new RobotsFilter();
        filter.addRules("Disallow: */prd14");

        System.out.println(filter.rules);
        System.out.println(filter.isAllowed("http://www.el-piano.com/#!product/prd14/3537846151/song-of-granada"));
    }
}
