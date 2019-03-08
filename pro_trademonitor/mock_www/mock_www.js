//功能：模拟用户注册、下单过程，直接往MySQL中insert记录

var mysql = require('mysql');
var Conf = require(__dirname + "/conf.js");

var conf = new Conf();

console.log("debug conf:" + conf.mysql.host);

function randomNum(Min, Max) {
    var Range = Max - Min;
    var Rand = Math.random();
    var num = Min + Math.round(Rand * Range); //四舍五入
    return num;
}

Date.prototype.format = function (fmt) {
    var o = {
        "M+": this.getMonth() + 1,                 //月份
        "d+": this.getDate(),                    //日
        "h+": this.getHours(),                   //小时
        "m+": this.getMinutes(),                 //分
        "s+": this.getSeconds(),                 //秒
        "q+": Math.floor((this.getMonth() + 3) / 3), //季度
        "S": this.getMilliseconds()             //毫秒
    };
    if (/(y+)/.test(fmt)) {
        fmt = fmt.replace(RegExp.$1, (this.getFullYear() + "").substr(4 - RegExp.$1.length));
    }
    for (var k in o) {
        if (new RegExp("(" + k + ")").test(fmt)) {
            fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (("00" + o[k]).substr(("" + o[k]).length)));
        }
    }
    return fmt;
}

function execSQL(tableName, sql, params) {
    var connection = mysql.createConnection({
        host: conf.mysql.host,
        user: conf.mysql.user,
        password: conf.mysql.password,
        database: conf.mysql.database
    });
    connection.connect();

    connection.query(sql, params, function (err, result) {
        if (err) {
            console.log('[INSERT ERROR] - ', err.message);
            return;
        }
        console.log("insert into  【 " + tableName + " 】 ... ");
    });

    connection.end();
}


function buildOrder() {
    var pId = randomNum(1, 3);
    var pName = 'iPhone';
    switch (pId) {
        case 1:
            pName = "iPhone";
            break;
        case 2:
            pName = "小米";
            break;
        case  3:
            pName = "OPPO";
            break;
    }

    var sql = 'INSERT INTO wuchen.order_detail(order_id,p_id,p_name,buy_count,total_price,create_time,update_time) VALUES(?,?,?,?,?,?,?)';
    var dt = new Date().format("yyyy-MM-dd hh:mm:ss");
    var buyCount = randomNum(1, 10);
    var price = randomNum(1000, 100000);
    var params = [randomNum(1, 10000), pId, pName, buyCount, price, dt, dt];

    execSQL("wuchen.order_detail", sql, params);

    var sql2 = 'insert into wuchen.`order`(user_id,order_status,total_price,order_time,create_time,update_time) values(?,?,?,?,?,?)';
    var params2 = [randomNum(1, 20), "payed", price, dt, dt, dt];

    execSQL("wuchen.order", sql2, params2);
}

function buildUser() {
    var sql = 'insert into wuchen.`user`(user_name,mobile_num,register_time,city,create_time,update_time) values(?,?,?,?,?,?)';
    var dt = new Date().format("yyyy-MM-dd hh:mm:ss");
    var cityId = randomNum(1, 5);

    var cityList = ["秦皇岛", "株洲", "石家庄", "武汉", "大庆", "合肥"];

    var params = ["wuchen" + cityId, "110", dt, cityList[cityId], dt, dt];

    execSQL("wuchen.user", sql, params);
}

//每3秒注册一个用户
var myInterval = setInterval(buildUser, 1000 * 3);

//每5秒生成一个订单项
var myInterval2 = setInterval(buildOrder, 1000 * 5);

function stopInterval() {
    clearTimeout(myInterval);
    clearTimeout(myInterval2);
}

//10分钟后关闭模拟程序
setTimeout(stopInterval, 1000 * 60 * 10);


