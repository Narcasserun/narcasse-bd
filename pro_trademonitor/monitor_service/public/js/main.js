// 功能:接受服务端的数据并绘制图表

$(document).ready(function () {
    Highcharts.setOptions({
        global: {
            useUTC: false
        },
        lang: {
            thousandsSep: ','
        }
    });

    var lineChart1 = new Highcharts.Chart(lineOption_1);
    var lineChart2 = new Highcharts.Chart(lineOption_2);

    var mapChart = echarts.init(document.getElementById('mapChart'));
    var pieChart = echarts.init(document.getElementById('pieChart'));

    mapChart.setOption(mapOption, true);
    pieChart.setOption(pieOption, true);

    var socket = io();
    socket.on('metric', function (row) {
        console.log("metric:" + row);
        var json = JSON.parse(row);
        var metricType = json["metric"];
        switch (metricType) {
            case "TotalPrice":
                redrawLine1(json);
                break;
            case "PriceByPrd":
                redrawLine2(json);
                break;
            case "RegisterUserCnt":
                redrawMap(json);
                break;
            case "UserRatio":
                redrawPie(json);
                break;
            default:
                console.warn(" invalid metric:" + metricType)
        }

        $("#timing").text(moment().format("YYYY-MM-DD HH:mm:ss"));
        $("#timing").fadeOut(100).fadeIn(100)
    });

    function getTimeStamp(dtStr) {
        return new Date(dtStr).getTime();
    }

    function redrawLine1(row) {
        var dt = getTimeStamp(row["dt"]);
        var v = [dt, row["price"]];
        console.info("total_price:" + v);
        lineChart1.series[0].addPoint(v, false, true);
        lineChart1.redraw();
    }

    function redrawLine2(row) {
        var seriesMap = {"iPhone": 0, "小米": 1, "OPPO": 2};
        var seriesId = seriesMap[row["pName"]];
        var v = [getTimeStamp(row["dt"]), row["price"]];
        lineChart2.series[seriesId].addPoint(v, false, true);
        lineChart2.redraw();
    }

    function redrawMap(row) {
        var v = {
            "name": row["city"],
            "value": geoCoordMap[row["city"]].concat(row["userCnt"] * 10) //放大100倍
        };

        mapOption.series[0].row = updateList(mapOption.series[0].data, v);
        mapChart.setOption(mapOption);
    }

    function redrawPie(row) {
        var v = {
            "name": row["userType"],
            "value": row["userCnt"]
        };
        pieOption.series[0].row = updateList(pieOption.series[0].data, v);
        pieChart.setOption(pieOption);
    }

    function updateList(list, element) {
        var bList = list;
        for (var i = 0; i < list.length; i++) {
            if (list[i].name == element.name) {
                list[i] = element;
                return bList;
            }
        }
    }
});