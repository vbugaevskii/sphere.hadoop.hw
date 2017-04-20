package ru.mail.sphere.hadoop.hbase;

import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;

public class TestRobotsFilterBugs {
    @Test
    public void test00() throws RobotsFilter.BadFormatException {
        String disallow = "Disallow: */2015\n" +
                "Disallow: */uploads/2015/02\n" +
                "Disallow: /wp-content/uploads/2015/02";

        RobotsFilter filter = new RobotsFilter(disallow);
        assertTrue(filter.isAllowed("http://2015.sarkfolkfestival.com/"));
    }

    @Test
    public void test01() throws RobotsFilter.BadFormatException {
        RobotsFilter filter = new RobotsFilter();

        filter.addRules("Disallow: /category/katalog");
        filter.addRules("Disallow: *?name=gbook");
        filter.addRules("Disallow: */coins_russia");

        assertFalse(filter.isAllowed("http://01center.ru/category/katalog/page/2/"));
        assertFalse(filter.isAllowed("http://0902.kmr.msudrf.ru/modules.php?name=gbook&page=18"));
        assertFalse(filter.isAllowed("http://21coins.ru/coins/coins_russian_empire/coins-aleksander1/"));
    }

    @Test
    public void test02() throws RobotsFilter.BadFormatException {
        String disallow = "Disallow: /upload/site1\n" +
                "Disallow: /ufms\n" +
                "Disallow: */index.php$\n" +
                "Disallow: */foreign/new\n" +
                "Disallow: */new";
        RobotsFilter filter = new RobotsFilter(disallow);

        assertTrue(filter.isAllowed("http://36.fms.gov.ru/legislation/ufms-rossii/"));
    }

    @Test
    public void test03() throws RobotsFilter.BadFormatException {
        RobotsFilter filter = new RobotsFilter("Disallow: /index");
        assertTrue(filter.isAllowed("http://51.eadres.info/company/index/50509549"));
        assertTrue(filter.isAllowed("http://51.eadres.info/rubric/index/15"));
    }

    @Test
    public void test04() throws RobotsFilter.BadFormatException {
        RobotsFilter filter = new RobotsFilter("Disallow: */goods$");
        assertFalse(filter.isAllowed("http://13258.ua.all.biz/en/goods"));
    }

    @Test
    public void test05() throws RobotsFilter.BadFormatException {
        RobotsFilter filter = new RobotsFilter("Disallow: /news");
        assertFalse(filter.isAllowed("http://1-gipermarket-kolyasok.ru/news?view=11147406"));
    }

    @Test
    public void test06() throws RobotsFilter.BadFormatException {
        String disallow = "Disallow: */product\n" +
                "Disallow: /_p/prd14\n" +
                "Disallow: */prd14\n";
        RobotsFilter filter = new RobotsFilter(disallow);
        assertFalse(filter.isAllowed("http://www.el-piano.com/#!product/prd14/3537846151/song-of-granada"));

        filter = new RobotsFilter("Disallow: /orphus/orphus.htm$");
        assertTrue(filter.isAllowed("http://kabriolet.ru/orphus/orphus.htm#!akrboiel@takrboiel.tur"));

        filter = new RobotsFilter("Disallow: /p4680869-gorshki-dlya-tsvetov.html$");
        assertTrue(filter.isAllowed("http://gorshok.kiev.ua/p4680869-gorshki-dlya-tsvetov.html#!"));

        disallow = "Disallow: *&testId=15187$\n" +
                "Disallow: /catalog\n" +
                "Disallow: /category\n" +
                "Disallow: *&testId=15121$\n" +
                "Disallow: *&testId=12549$\n" +
                "Disallow: /task";
        filter = new RobotsFilter(disallow);
        assertTrue(filter.isAllowed("http://www.memotest.ru/test?cardId=124368&testId=15121#!"));
    }
}
