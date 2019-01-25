from src.commons.date_utils import utc_timestamp

from .base_mongo import db



class PushStrategy(db.Document):
    """推送信息服务"""
    meta = {"collection": "support_push_strategy","strict": False}  # 集合名
    push_type = db.StringField(required=True, verbose_name="推送类型")
    sub_type = db.StringField(required=True, default="", verbose_name="推送子类型")
    status = db.IntField(required=True, default=1,verbose_name="推送状态")#0不推送，1推送
    code=db.StringField(required=True, verbose_name="推送编码")#与推送类型 推送子类型 三者唯一
    desc=db.StringField(required=True, verbose_name="推送描述")
    operator = db.StringField(required=True, verbose_name="操作人姓名")
    operator_id = db.StringField(required=True, verbose_name="操作人ID")
    create_time = db.IntField(required=True, default=utc_timestamp, verbose_name="创建时间")
    update_time = db.IntField(required=True, default=utc_timestamp, verbose_name="更新时间")
    #返回的字段
    def to_pretty(self):
        return {
            'id':str(self.id),
            'type':self.push_type,
            'sub_type':self.sub_type or "",
            'code':self.code or "",
            'desc':self.desc,
            'status':self.status,
            'operator':self.operator,
            'operator_id':self.operator_id
        }
