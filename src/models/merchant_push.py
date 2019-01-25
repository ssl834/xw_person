from src import db
from src.commons.date_utils import utc_timestamp
class  TbMerchantPush(db.Model):
    """推送策略关联商户"""
    __tablename__ = "tb_merchant_push"
    id = db.Column(db.Integer, primary_key=True, doc="自增 id", autoincrement=True)
    merchant_code=db.Column(db.String(32),doc="商户号")
    type=db.Column(db.String(64),doc="推送类型 ")
    push_method=db.Column(db.String(32),doc="推送方式")
    username=db.Column(db.String(128), doc="推送账户")
    password=db.Column(db.String(100), doc="密码")
    status=db.Column(db.Boolean,default=True,doc="是否开启")
    operator = db.Column(db.String(64), doc="操作人姓名")
    operator_id = db.Column(db.String(64), doc="操作人ID")
    create_time = db.Column(db.BigInteger, default=utc_timestamp, doc="创建时间")
    update_time = db.Column(db.BigInteger, default=utc_timestamp, doc="更新时间")


    def to_pretty(self):
        return {
            'id':str(self.id),
            'merchant_code':self.merchant_code,
            'type':self.type,
            'push_method':self.push_method,
            'username':self.username,
            'password':self.password,
            'status':1 if self.status else 0 ,#统一返回数字
            'operator':self.operator,
            'operator_id':self.operator_id
        }