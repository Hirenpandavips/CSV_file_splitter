module.exports = (dbConnection, Sequelize) => {
  const contact = dbConnection.define(
    'contact',
    {
      id: {
        type: Sequelize.INTEGER,
        primaryKey: true,
        autoIncrement: true,
        allowNull: false,
      },
      email: {
        type: Sequelize.STRING,
        unique: true,
        allowNull: false,
      },
      designation: {
        type: Sequelize.STRING,
      },
    },
    {
      timestamps: true,
    }
  )
  contact.associate = (models) => {
    contact.belongsTo(models.company, {
      foreignKey: 'companyId',
      onDelete: 'CASCADE',
    })
  }
  return contact
}
