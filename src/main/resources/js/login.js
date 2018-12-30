function login_click(){
	var uid = document.getElementById('email').value;
	var pwd = document.getElementById('password').value;
	if(uid == "" || pwd == "" ){
		alert("输入的用户名或密码不能为空");
		return false;
	}
	if(pwd.length < 6 || pwd.length > 20){
		alert("输入的密码必须为6-20位");
		return false;
	}
	if( !(uid == "") || !(psw == "")){
		
		alert("登陆成功");
		window.location="index.html";
	}
	
}
function submit(){
	alert("aa");
	var count = 0;
	var checkArray = document.getElementsByName("checkbox");
	for (var i = 0;i < checkaArray.length;i ++) {
		if(checkArray[i].checked == true){
			count++;
			}
	}
	if(count = 0){
		alert("请至少选择一项！");
	}
}