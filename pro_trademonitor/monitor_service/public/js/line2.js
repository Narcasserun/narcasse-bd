//功能：绘制曲线图，各产品分时成交量

Highcharts.setOptions({
    global: {
        useUTC: false
    },
    lang: {
        thousandsSep: ','
    }
});

function initData() {
    // generate an array of random data
    var data = [],
        time = (new Date()).getTime(),
        i;
    for (i = -20; i <= 0; i += 1) {
        data.push({
            x: time + i * 1000,
            y: 0
        });
    }
    return data;
}

lineOption_2 = {
    chart: {
        renderTo: 'lineChart2',
        type: 'spline',
        animation: Highcharts.svg, // don't animate in old IE
        marginRight: 10,
        backgroundColor: null
    },
    title: {
        text: '各产品分时成交量',
        style: {
            color: "#C0C0C0"
        }
    },
    xAxis: {
        type: 'datetime',
        tickPixelInterval: 150,
        title: {
            text: '',
            style: {
                fontWeight: 'bold'
            }
        }
    },
    yAxis: {
        title: {
            text: '成交量(元)',
            style: {
                fontWeight: 'bold'
            }
        },
        plotLines: [{
            value: 0,
            width: 1,
            color: '#808080'
        }]
    },
    tooltip: {
        formatter: function () {
            return '<b>' + this.series.name + '</b><br/>时间段:' +
                Highcharts.dateFormat('%H:%M:%S', this.x) + '<br/>成交量:' + Highcharts.numberFormat(this.y, 0);
        }
    },
    legend: {
        enabled: true,
        itemStyle: {cursor: 'pointer', color: '#3E576F'}

    },
    exporting: {
        enabled: false
    },
    credits: {
        enabled: false
    },
    series: [
        {
            name: 'iPhone',
            data: (initData())
        },
        {
            name: '小米',
            data: (initData()),
            color: '#F0E68C'
        },
        {
            name: 'OPPO',
            data: (initData()),
            color: '#ADFF2F'
        }
    ]
}